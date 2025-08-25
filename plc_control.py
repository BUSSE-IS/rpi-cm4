#!/usr/bin/env python3
"""
Improved IoT Control System with Power Monitoring and ESP32 Integration
Author: Refactored for better efficiency and maintainability
"""

import json
import ssl
import time
import os
import threading
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import copy

# Third-party imports
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
import schedule
import psutil
from pymodbus.client import ModbusTcpClient
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusException
import httplib2

# Local imports (assuming these exist)
try:
    from sensors import temp, ana
    SENSORS_AVAILABLE = True
except ImportError:
    SENSORS_AVAILABLE = False
    logging.warning("Sensors module not available")

try:
    import g_lcd
    LCD_AVAILABLE = True
except ImportError:
    LCD_AVAILABLE = False
    logging.warning("LCD module not available")


# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/iot_control.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class PowerData:
    """Data structure for power sensor readings"""
    voltage: float = 0.0
    current: float = 0.0
    power: float = 0.0
    energy: float = 0.0
    frequency: float = 0.0
    power_factor: float = 0.0


class PowerSensor:
    """Eastron SDM230 Power Sensor Interface"""
    
    def __init__(self, port: str = '/dev/ttyUSB0', baudrate: int = 9600, slave_id: int = 1):
        self.client = ModbusSerialClient(
            method='rtu',
            port=port,
            baudrate=baudrate,
            stopbits=1,
            bytesize=8,
            parity='N',
            timeout=1
        )
        self.slave_id = slave_id
        self.connected = False
        
    def connect(self) -> bool:
        """Connect to the power sensor"""
        try:
            self.connected = self.client.connect()
            if self.connected:
                logging.info("Connected to power sensor")
            return self.connected
        except Exception as e:
            logging.error(f"Failed to connect to power sensor: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from the power sensor"""
        if self.client:
            self.client.close()
            self.connected = False
    
    def read_data(self) -> PowerData:
        """Read data from SDM230 power meter"""
        if not self.connected:
            return PowerData()
        
        try:
            # SDM230 register addresses (holding registers, read with function code 4)
            registers = {
                'voltage': 0x0000,      # Voltage (V)
                'current': 0x0006,      # Current (A)  
                'power': 0x000C,        # Active Power (W)
                'energy': 0x0156,       # Total Energy (kWh)
                'frequency': 0x0046,    # Frequency (Hz)
                'power_factor': 0x001E  # Power Factor
            }
            
            data = PowerData()
            
            for param, address in registers.items():
                try:
                    result = self.client.read_holding_registers(
                        address, 2, slave=self.slave_id
                    )
                    if not result.isError():
                        # Convert two 16-bit registers to float (IEEE 754)
                        raw_value = (result.registers[0] << 16) | result.registers[1]
                        float_value = self._raw_to_float(raw_value)
                        setattr(data, param, float_value)
                except Exception as e:
                    logging.error(f"Error reading {param}: {e}")
            
            return data
            
        except Exception as e:
            logging.error(f"Error reading power sensor data: {e}")
            return PowerData()
    
    def _raw_to_float(self, raw_value: int) -> float:
        """Convert raw 32-bit integer to IEEE 754 float"""
        import struct
        return struct.unpack('>f', struct.pack('>I', raw_value))[0]


class ESP32MQTTHandler:
    """Handler for ESP32 MQTT communication using external broker"""
    
    def __init__(self, parent_system, broker_host: str = 'localhost', broker_port: int = 1884):
        self.parent_system = parent_system
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.esp32_client = None
        self.running = False
        
        # Topics for ESP32 communication
        self.command_topic = "esp32/command"
        self.response_topic = "esp32/response"
        self.status_topic = "esp32/status"
    
    def start(self):
        """Start ESP32 MQTT handler"""
        try:
            self.esp32_client = mqtt.Client(client_id="svs_esp32_handler")
            self.esp32_client.on_connect = self._on_esp32_connect
            self.esp32_client.on_message = self._on_esp32_message
            
            self.esp32_client.connect(self.broker_host, self.broker_port, 60)
            self.esp32_client.loop_start()
            self.running = True
            
            logging.info(f"ESP32 MQTT handler started on {self.broker_host}:{self.broker_port}")
        except Exception as e:
            logging.error(f"Failed to start ESP32 MQTT handler: {e}")
    
    def stop(self):
        """Stop ESP32 MQTT handler"""
        self.running = False
        if self.esp32_client:
            self.esp32_client.loop_stop()
            self.esp32_client.disconnect()
        logging.info("ESP32 MQTT handler stopped")
    
    def _on_esp32_connect(self, client, userdata, flags, rc):
        """Callback for ESP32 MQTT connection"""
        if rc == 0:
            logging.info("Connected to ESP32 MQTT broker")
            client.subscribe(self.command_topic)
            client.subscribe(f"{self.command_topic}/+")  # Subscribe to device-specific commands
        else:
            logging.error(f"Failed to connect to ESP32 MQTT broker: {rc}")
    
    def _on_esp32_message(self, client, userdata, msg):
        """Handle messages from ESP32 devices"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            logging.info(f"ESP32 message received - Topic: {topic}, Payload: {payload}")
            
            # Extract device ID from topic if present
            device_id = "unknown"
            if "/" in topic:
                parts = topic.split("/")
                if len(parts) > 2:
                    device_id = parts[2]
            
            response = self.handle_esp32_command(payload, device_id)
            
            # Send response back to ESP32
            response_topic = f"{self.response_topic}/{device_id}"
            client.publish(response_topic, json.dumps(response))
            
        except Exception as e:
            logging.error(f"Error handling ESP32 message: {e}")
    
    def handle_esp32_command(self, payload: str, device_id: str) -> Dict[str, Any]:
        """Handle commands from ESP32 devices"""
        try:
            data = json.loads(payload)
            command = data.get('command', '')
            
            logging.info(f"Processing command '{command}' from ESP32 device {device_id}")
            
            if command == 'rebootSVS':
                return self._handle_reboot()
            elif command == 'getStatus':
                return self._handle_get_status()
            elif command == 'setPower':
                return self._handle_set_power(data.get('state', False))
            elif command == 'getCounters':
                return self._handle_get_counters()
            elif command == 'resetCounters':
                return self._handle_reset_counters()
            elif command == 'getPowerData':
                return self._handle_get_power_data()
            elif command == 'setGPIO':
                return self._handle_set_gpio(data.get('pin'), data.get('state'))
            else:
                return {'status': 'error', 'message': f'Unknown command: {command}'}
                
        except json.JSONDecodeError:
            return {'status': 'error', 'message': 'Invalid JSON payload'}
        except Exception as e:
            return {'status': 'error', 'message': f'Command processing error: {str(e)}'}
    
    def _handle_reboot(self) -> Dict[str, str]:
        """Handle reboot command"""
        logging.info("Reboot command received from ESP32")
        threading.Timer(5.0, lambda: os.system('sudo reboot')).start()
        return {'status': 'success', 'message': 'Rebooting in 5 seconds'}
    
    def _handle_get_status(self) -> Dict[str, Any]:
        """Handle status request"""
        ram = psutil.virtual_memory()
        return {
            'status': 'success',
            'data': {
                'uptime': str(self._get_uptime()),
                'cpu_usage': psutil.cpu_percent(),
                'memory_usage': ram.percent,
                'free_ram_mb': ram.free / 1024 / 1024,
                'cpu_temp': self.parent_system._measure_temp(),
                'mqtt_connected': self.parent_system.mqtt_client.connected_flag if self.parent_system.mqtt_client else False,
                'modbus_connected': self.parent_system.modbus_client.connected_flag if self.parent_system.modbus_client else False,
                'power_sensor_connected': self.parent_system.power_sensor.connected if self.parent_system.power_sensor else False,
                'timestamp': datetime.now().isoformat()
            }
        }
    
    def _handle_get_counters(self) -> Dict[str, Any]:
        """Handle get counters request"""
        return {
            'status': 'success',
            'data': {
                'counters': self.parent_system.counters,
                'flow_data': {str(k): v for k, v in self.parent_system.flow_data.items()},
                'timestamp': datetime.now().isoformat()
            }
        }
    
    def _handle_reset_counters(self) -> Dict[str, str]:
        """Handle reset counters request"""
        try:
            for counter in self.parent_system.counters:
                self.parent_system.counters[counter] = 0
            
            for flow_sensor in self.parent_system.flow_data:
                self.parent_system.flow_data[flow_sensor]['ges'] = 0
            
            logging.info("Counters reset by ESP32 command")
            return {'status': 'success', 'message': 'Counters reset successfully'}
        except Exception as e:
            return {'status': 'error', 'message': f'Failed to reset counters: {str(e)}'}
    
    def _handle_get_power_data(self) -> Dict[str, Any]:
        """Handle get power data request"""
        if self.parent_system.power_sensor and self.parent_system.power_sensor.connected:
            power_data = self.parent_system.power_sensor.read_data()
            return {
                'status': 'success',
                'data': {
                    'voltage': power_data.voltage,
                    'current': power_data.current,
                    'power': power_data.power,
                    'energy': power_data.energy,
                    'frequency': power_data.frequency,
                    'power_factor': power_data.power_factor,
                    'timestamp': datetime.now().isoformat()
                }
            }
        else:
            return {'status': 'error', 'message': 'Power sensor not available'}
    
    def _handle_set_power(self, state: bool) -> Dict[str, str]:
        """Handle power control command"""
        # This would control a relay or power switch connected to GPIO
        try:
            # Example: Control a relay on GPIO pin 18
            power_pin = 18
            GPIO.setup(power_pin, GPIO.OUT)
            GPIO.output(power_pin, GPIO.HIGH if state else GPIO.LOW)
            
            logging.info(f"Power state set to {state} by ESP32 command")
            return {'status': 'success', 'message': f'Power state set to {state}'}
        except Exception as e:
            return {'status': 'error', 'message': f'Failed to set power state: {str(e)}'}
    
    def _handle_set_gpio(self, pin: int, state: bool) -> Dict[str, str]:
        """Handle GPIO control command"""
        try:
            if pin is None:
                return {'status': 'error', 'message': 'Pin number required'}
            
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.HIGH if state else GPIO.LOW)
            
            logging.info(f"GPIO pin {pin} set to {state} by ESP32 command")
            return {'status': 'success', 'message': f'GPIO {pin} set to {state}'}
        except Exception as e:
            return {'status': 'error', 'message': f'Failed to set GPIO {pin}: {str(e)}'}
    
    def _get_uptime(self) -> timedelta:
        """Get system uptime"""
        return datetime.now() - datetime.fromtimestamp(psutil.boot_time())
    
    def send_notification_to_esp32(self, device_id: str, message: str, data: Dict[str, Any] = None):
        """Send notification to specific ESP32 device"""
        if not self.running or not self.esp32_client:
            return
        
        try:
            notification = {
                'type': 'notification',
                'message': message,
                'timestamp': datetime.now().isoformat()
            }
            
            if data:
                notification['data'] = data
            
            topic = f"esp32/notification/{device_id}"
            self.esp32_client.publish(topic, json.dumps(notification))
            
        except Exception as e:
            logging.error(f"Failed to send notification to ESP32 {device_id}: {e}")


class IoTControlSystem:
    """Main IoT Control System"""
    
    def __init__(self, settings_file: str = '/boot/svs/settings.json'):
        self.settings_file = Path(settings_file)
        self.data_file = Path('/boot/svs/offline_data.json')
        
        # Initialize data structures
        self.settings: Dict[str, Any] = {}
        self.data: Dict[str, Any] = {}
        self.data_previous: Dict[str, Any] = {}
        self.counters: Dict[str, float] = {}
        self.last_counters: Dict[str, float] = {}
        self.flow_data: Dict[int, Dict[str, Any]] = {}
        self.last_flow_data: Dict[int, Dict[str, Any]] = {}
        self.sensor_data: Dict[str, float] = {}
        
        # Initialize sensor objects
        self.tds_sensors: Dict[str, Any] = {}
        self.pressure_sensors: Dict[str, Any] = {}
        self.turbidity_sensors: Dict[str, Any] = {}
        
        # Initialize connections
        self.mqtt_client: Optional[mqtt.Client] = None
        self.modbus_client: Optional[ModbusTcpClient] = None
        self.power_sensor: Optional[PowerSensor] = None
        self.esp32_handler: Optional[ESP32MQTTHandler] = None
        
        # System state
        self.running = False
        self.start_counter = 0
        self.lcd_count = 0
        
        # Setup GPIO
        GPIO.setmode(GPIO.BCM)
        
        # Initialize components
        self.load_settings()
        self.setup_mqtt()
        self.setup_modbus()
        self.setup_power_sensor()
        self.setup_esp32_handler()
        self.setup_sensors()
        self.setup_scheduler()
    
    def load_settings(self):
        """Load settings from JSON file"""
        try:
            if self.settings_file.exists():
                with open(self.settings_file, 'r') as f:
                    self.settings = json.load(f)
                logging.info("Settings loaded successfully")
            else:
                logging.warning(f"Settings file not found: {self.settings_file}")
                self.settings = self.get_default_settings()
        except Exception as e:
            logging.error(f"Error loading settings: {e}")
            self.settings = self.get_default_settings()
    
    def get_default_settings(self) -> Dict[str, Any]:
        """Get default settings"""
        return {
            "edit": False,
            "general": {
                "ip": "192.168.119.12",
                "apikey": ""
            },
            "mqtt": {
                "broker": "mqtt.busse-board.de",
                "port": 8883,
                "username": "",
                "password": "",
                "device_id": "device001"
            },
            "power_sensor": {
                "enabled": False,
                "port": "/dev/ttyUSB0",
                "baudrate": 9600,
                "slave_id": 1
            },
            "esp32_broker": {
                "enabled": False,
                "port": 1884
            },
            "remote_devices": {},
            "modbus_data": [],
            "pin_settings": {
                "action": [],
                "counter": []
            },
            "sensors": {}
        }
    
    def save_settings(self, new_settings: Dict[str, Any]):
        """Save settings to JSON file"""
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(new_settings, f, indent=2)
            logging.info("Settings saved successfully")
        except Exception as e:
            logging.error(f"Error saving settings: {e}")
    
    def setup_mqtt(self):
        """Setup MQTT client"""
        try:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.connected_flag = False
            self.mqtt_client.disconnected_flag = True
            
            # Setup callbacks
            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_disconnect = self._on_disconnect
            self.mqtt_client.on_message = self._on_message
            self.mqtt_client.message_callback_add("v1/devices/me/attributes", self._on_message_attr)
            self.mqtt_client.message_callback_add("v1/devices/me/rpc/request/+", self._on_message_req)
            self.mqtt_client.message_callback_add("v1/devices/me/attributes/response/+", self._on_message_resp)
            
            # Setup TLS if certificates exist
            cert_files = ["mqttserver.pub.pem", "cert.pem", "key.pem"]
            if all(Path(f).exists() for f in cert_files):
                self.mqtt_client.tls_set(
                    cert_files[0], cert_files[1], cert_files[2],
                    cert_reqs=ssl.CERT_REQUIRED,
                    tls_version=ssl.PROTOCOL_TLSv1_2
                )
                self.mqtt_client.tls_insecure_set(False)
            
            # Connect
            mqtt_config = self.settings.get('mqtt', {})
            broker = mqtt_config.get('broker', 'mqtt.busse-board.de')
            port = mqtt_config.get('port', 8883)
            
            self.mqtt_client.connect_async(broker, port, 60)
            self.mqtt_client.loop_start()
            
        except Exception as e:
            logging.error(f"Error setting up MQTT: {e}")
    
    def setup_modbus(self):
        """Setup Modbus client"""
        try:
            ip = self.settings.get('general', {}).get('ip', '192.168.119.12')
            self.modbus_client = ModbusTcpClient(ip, 502)
            self.modbus_client.connected_flag = False
            
            if self.modbus_client.connect():
                logging.info('Connected to Modbus Server')
                self.modbus_client.connected_flag = True
            else:
                logging.warning('Failed to connect to Modbus Server')
                
        except Exception as e:
            logging.error(f"Error setting up Modbus: {e}")
    
    def setup_power_sensor(self):
        """Setup power sensor if enabled"""
        power_config = self.settings.get('power_sensor', {})
        if power_config.get('enabled', False):
            try:
                self.power_sensor = PowerSensor(
                    port=power_config.get('port', '/dev/ttyUSB0'),
                    baudrate=power_config.get('baudrate', 9600),
                    slave_id=power_config.get('slave_id', 1)
                )
                self.power_sensor.connect()
            except Exception as e:
                logging.error(f"Error setting up power sensor: {e}")
    
    def setup_esp32_handler(self):
        """Setup ESP32 MQTT handler if enabled"""
        esp32_config = self.settings.get('esp32_broker', {})
        if esp32_config.get('enabled', False):
            try:
                broker_host = esp32_config.get('host', 'localhost')
                broker_port = esp32_config.get('port', 1884)
                
                self.esp32_handler = ESP32MQTTHandler(
                    parent_system=self,
                    broker_host=broker_host,
                    broker_port=broker_port
                )
                self.esp32_handler.start()
            except Exception as e:
                logging.error(f"Error setting up ESP32 handler: {e}")
    
    def setup_sensors(self):
        """Setup various sensors"""
        if not SENSORS_AVAILABLE:
            return
        
        sensors_config = self.settings.get('sensors', {})
        
        # Setup flow sensors
        if 'flow_sensors' in sensors_config:
            self._setup_flow_sensors(sensors_config['flow_sensors'])
        
        # Setup TDS sensors
        if 'tds_sensors' in sensors_config:
            self._setup_tds_sensors(sensors_config['tds_sensors'])
        
        # Setup pressure sensors
        if 'pressure_sensors' in sensors_config:
            self._setup_pressure_sensors(sensors_config['pressure_sensors'])
        
        # Setup turbidity sensors
        if 'turbidity_sensors' in sensors_config:
            self._setup_turbidity_sensors(sensors_config['turbidity_sensors'])
    
    def _setup_flow_sensors(self, flow_sensors_config: Dict[str, Any]):
        """Setup flow sensors"""
        for sensor_name, config in flow_sensors_config.items():
            pin = config['pin']
            try:
                GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                GPIO.add_event_detect(pin, GPIO.FALLING, callback=self._count_flow_pulse)
                
                self.flow_data[pin] = {
                    'ges': 0,
                    'rate': 0,
                    'count': 0,
                    'state': True,
                    'ppl': config.get('ppl', 5880)
                }
                logging.info(f"Flow sensor {sensor_name} setup on pin {pin}")
            except Exception as e:
                logging.error(f"Error setting up flow sensor {sensor_name}: {e}")
    
    def _setup_tds_sensors(self, tds_sensors_config: Dict[str, Any]):
        """Setup TDS sensors"""
        for sensor_name, port in tds_sensors_config.items():
            try:
                self.tds_sensors[sensor_name] = ana.GroveTDS(int(port))
                logging.info(f"TDS sensor {sensor_name} setup on port {port}")
            except Exception as e:
                logging.error(f"Error setting up TDS sensor {sensor_name}: {e}")
    
    def _setup_pressure_sensors(self, pressure_sensors_config: Dict[str, Any]):
        """Setup pressure sensors"""
        for sensor_name, port in pressure_sensors_config.items():
            try:
                self.pressure_sensors[sensor_name] = ana.GroveTDS(int(port))
                logging.info(f"Pressure sensor {sensor_name} setup on port {port}")
            except Exception as e:
                logging.error(f"Error setting up pressure sensor {sensor_name}: {e}")
    
    def _setup_turbidity_sensors(self, turbidity_sensors_config: Dict[str, Any]):
        """Setup turbidity sensors"""
        for sensor_name, port in turbidity_sensors_config.items():
            try:
                self.turbidity_sensors[sensor_name] = ana.GroveTDS(int(port))
                logging.info(f"Turbidity sensor {sensor_name} setup on port {port}")
            except Exception as e:
                logging.error(f"Error setting up turbidity sensor {sensor_name}: {e}")
    
    def setup_scheduler(self):
        """Setup scheduled tasks"""
        schedule.every().hours.do(self.publish_cycle)
        schedule.every(30).seconds.do(self.check_plc_status)
        schedule.every(30).minutes.do(self.save_cycle)
        schedule.every(30).seconds.do(self.check_modbus_connection)
        
        # Setup LCD if available
        if LCD_AVAILABLE:
            try:
                g_lcd.boot_display()
                schedule.every(3).seconds.do(self.update_lcd)
                logging.info("LCD display initialized")
            except Exception as e:
                logging.error(f"Error initializing LCD: {e}")
    
    # MQTT Callbacks
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connect callback"""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.connected_flag = True
            client.disconnected_flag = False
            
            # Subscribe to topics
            client.subscribe("v1/devices/me/attributes")
            client.subscribe("v1/devices/me/rpc/request/+")
            client.subscribe('v1/devices/me/attributes/response/+')
            
            # Request shared attributes
            client.publish('v1/devices/me/attributes/request/1', '{"sharedKeys":"settings"}', 1)
            client.publish('v1/devices/me/attributes/request/2', '{"sharedKeys":"counters"}', 1)
            client.publish('v1/devices/me/attributes/request/3', '{"sharedKeys":"flow_data"}', 1)
        else:
            logging.error(f"Failed to connect to MQTT broker, code: {rc}")
            client.connected_flag = False
            client.disconnected_flag = True
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback"""
        if not client.disconnected_flag:
            client.connected_flag = False
            client.disconnected_flag = True
            logging.warning("Disconnected from MQTT broker")
            
            # Reset counters to prevent data loss
            for counter in self.counters:
                self.counters[counter] -= self.last_counters[counter]
            
            for flow_sensor in self.flow_data:
                self.flow_data[flow_sensor]['ges'] -= self.last_flow_data[flow_sensor]['ges']
    
    def _on_message(self, client, userdata, msg):
        """Default MQTT message callback"""
        logging.debug(f"Received message: {msg.topic} - {msg.payload}")
    
    def _on_message_attr(self, client, userdata, msg):
        """Handle attribute messages"""
        try:
            payload_data = json.loads(msg.payload)
            
            if 'counters' in payload_data:
                logging.info(f"Received counters: {payload_data['counters']}")
                for key, value in payload_data['counters'].items():
                    if key in self.counters:
                        self.counters[key] = value
            
            if 'flow_data' in payload_data:
                logging.info(f"Received flow_data: {payload_data['flow_data']}")
                for key, value in payload_data['flow_data'].items():
                    int_key = int(key)
                    if int_key in self.flow_data:
                        self.flow_data[int_key]['ges'] += value['ges']
            
            if 'settings' in payload_data:
                if self._settings_changed(payload_data['settings']):
                    logging.info("Settings changed, updating...")
                    self.save_settings(payload_data['settings'])
                    self.load_settings()
                    self.save_cycle()
            
            self.publish_data()
            
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding attribute message: {e}")
    
    def _on_message_req(self, client, userdata, msg):
        """Handle RPC requests"""
        try:
            payload_data = json.loads(msg.payload)
            method = payload_data.get('method', '')
            
            response_topic = msg.topic.replace('request', 'response')
            
            if method == 'getGpioStatus':
                client.publish(response_topic, '{"message":"Sending GPIO status."}', 1)
                self.publish_data()
            elif method == 'reboot':
                client.publish(response_topic, '{"message":"Rebooting in 5 seconds!"}', 1)
                threading.Timer(5.0, lambda: os.system('sudo reboot')).start()
            elif method == 'shutdown':
                client.publish(response_topic, '{"message":"Shutdown in 5 seconds!"}', 1)
                threading.Timer(5.0, lambda: os.system('sudo shutdown -h now')).start()
            elif method == 'pubTrigger':
                client.publish(response_topic, '{"message":"Publishing data."}', 1)
                self.publish_cycle()
            else:
                client.publish(response_topic, '{"message":"Request not recognized."}', 1)
                
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding RPC request: {e}")
    
    def _on_message_resp(self, client, userdata, msg):
        """Handle response messages"""
        try:
            payload_data = json.loads(msg.payload)
            if 'shared' in payload_data:
                self._handle_shared_attributes(payload_data['shared'])
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding response message: {e}")
    
    def _handle_shared_attributes(self, shared_data: Dict[str, Any]):
        """Handle shared attributes from server"""
        if 'counters' in shared_data:
            for key, value in shared_data['counters'].items():
                if key in self.counters:
                    self.counters[key] += value
        
        if 'flow_data' in shared_data:
            if 'flow_sensors' in self.settings['sensors']:
                for key, value in shared_data['flow_data'].items():
                    int_key = int(key)
                    if int_key in self.flow_data:
                        self.flow_data[int_key]['ges'] += value['ges']
        
        if 'settings' in shared_data:
            if self._settings_changed(shared_data['settings']):
                logging.info("Shared settings changed, updating...")
                self.save_settings(shared_data['settings'])
                self.load_settings()
                self.save_cycle()
    
    def _settings_changed(self, new_settings: Dict[str, Any]) -> bool:
        """Check if settings have changed"""
        current = copy.deepcopy(self.settings)
        new = copy.deepcopy(new_settings)
        
        # Remove IP from comparison as it might change
        current.get('general', {}).pop('ip', None)
        new.get('general', {}).pop('ip', None)
        
        return json.dumps(current, sort_keys=True) != json.dumps(new, sort_keys=True)
    
    # Flow sensor callback
    def _count_flow_pulse(self, channel: int):
        """Count flow pulses"""
        if self.start_counter == 1:
            if channel in self.flow_data:
                self.flow_data[channel]['count'] += 1
    
    # Data management methods
    def publish_data(self):
        """Publish current data to MQTT"""
        if not (self.mqtt_client and self.mqtt_client.connected_flag):
            return
        
        try:
            # Publish main data
            self.mqtt_client.publish('v1/devices/me/telemetry', json.dumps(self.data), 1)
            
            # Publish action data
            action_data = {}
            for action in self.settings.get('pin_settings', {}).get('action', []):
                name = action.get('name')
                pin = action.get('pin')
                if name and pin and pin in self.data:
                    action_data[name] = self.data[pin]
            
            if action_data:
                self.mqtt_client.publish('v1/devices/me/telemetry', json.dumps(action_data), 1)
            
            # Publish counters
            if self.counters:
                self.mqtt_client.publish('v1/devices/me/telemetry', json.dumps(self.counters), 1)
            
        except Exception as e:
            logging.error(f"Error publishing data: {e}")
    
    def publish_cycle(self):
        """Publish comprehensive system data"""
        if not (self.mqtt_client and self.mqtt_client.connected_flag):
            return
        
        try:
            # System information
            ram = psutil.virtual_memory()
            pub_values = self.counters.copy()
            
            # Format counter values
            for key in pub_values:
                if isinstance(pub_values[key], (int, float)):
                    pub_values[key] = f"{pub_values[key]:.2f}"
            
            # Add system stats
            pub_values.update({
                'cpu_usage': f"{psutil.cpu_percent():.2f}",
                'free_ram': ram.free / 1000000,
                'total_ram': ram.total / 1000000,
                'uptime': str(self._get_uptime()),
                'cpu_temp': self._measure_temp()
            })
            
            # Add sensor data
            self._add_sensor_data_to_publish(pub_values)
            
            # Add power sensor data if available
            if self.power_sensor and self.power_sensor.connected:
                power_data = self.power_sensor.read_data()
                pub_values.update({
                    'power_voltage': f"{power_data.voltage:.2f}",
                    'power_current': f"{power_data.current:.2f}",
                    'power_watts': f"{power_data.power:.2f}",
                    'power_energy': f"{power_data.energy:.2f}",
                    'power_frequency': f"{power_data.frequency:.2f}",
                    'power_factor': f"{power_data.power_factor:.2f}"
                })
            
            # Publish data
            self.mqtt_client.publish('v1/devices/me/telemetry', json.dumps(pub_values), 1)
            logging.info("Published cycle data")
            
        except Exception as e:
            logging.error(f"Error in publish cycle: {e}")
    
    def _add_sensor_data_to_publish(self, pub_values: Dict[str, Any]):
        """Add sensor data to publish values"""
        # Flow sensors
        if 'flow_sensors' in self.settings.get('sensors', {}):
            for flow_sensor in self.settings['sensors']['flow_sensors']:
                pin = self.settings['sensors']['flow_sensors'][flow_sensor]['pin']
                if pin in self.flow_data:
                    flow_ges = f"{flow_sensor}_ges"
                    flow_rate = f"{flow_sensor}_rate"
                    pub_values[flow_ges] = f"{self.flow_data[pin]['ges']:.2f}"
                    pub_values[flow_rate] = f"{self.flow_data[pin]['rate']:.2f}"
        
        # TDS sensors
        for tds_name, tds_sensor in self.tds_sensors.items():
            try:
                pub_values[tds_name] = f"{tds_sensor.TDS:.2f}"
            except:
                logging.warning(f"Failed to read TDS sensor {tds_name}")
        
        # Pressure sensors
        for pres_name, pres_sensor in self.pressure_sensors.items():
            try:
                pub_values[pres_name] = f"{pres_sensor.PRES:.2f}"
            except:
                logging.warning(f"Failed to read pressure sensor {pres_name}")
        
        # Turbidity sensors
        for turb_name, turb_sensor in self.turbidity_sensors.items():
            try:
                pub_values[turb_name] = f"{turb_sensor.TURB:.2f}"
            except:
                logging.warning(f"Failed to read turbidity sensor {turb_name}")
    
    def save_cycle(self):
        """Save data cycle to server or offline storage"""
        if self.mqtt_client and self.mqtt_client.connected_flag:
            try:
                # Save to server
                json_counters = json.dumps(self.counters)
                json_flow_data = json.dumps(self.flow_data)
                
                attributes_payload = {
                    "client": False,
                    "counters": self.counters,
                    "flow_data": self.flow_data
                }
                
                self.mqtt_client.publish(
                    'v1/devices/me/attributes', 
                    json.dumps(attributes_payload), 
                    1
                )
                
                # Update last saved values
                self.last_counters = self.counters.copy()
                self.last_flow_data = copy.deepcopy(self.flow_data)
                
                # Remove offline data file if it exists
                if self.data_file.exists():
                    self.data_file.unlink()
                    
            except Exception as e:
                logging.error(f"Error saving to server: {e}")
                
        else:
            # Save offline
            logging.info("Saving data offline")
            try:
                offline_data = {
                    'counters': self.counters,
                    'flow_data': self.flow_data,
                    'timestamp': datetime.now().isoformat()
                }
                
                with open(self.data_file, 'w') as f:
                    json.dump(offline_data, f, indent=2)
                    
            except Exception as e:
                logging.error(f"Error saving offline data: {e}")
    
    def load_offline_data(self):
        """Load offline data if available"""
        try:
            if self.data_file.exists():
                with open(self.data_file, 'r') as f:
                    offline_data = json.load(f)
                
                # Load counters
                for counter in self.counters:
                    if counter in offline_data['counters']:
                        self.counters[counter] = offline_data['counters'][counter]
                
                # Load flow data
                for flow_sensor in self.flow_data:
                    str_sensor = str(flow_sensor)
                    if str_sensor in offline_data['flow_data']:
                        self.flow_data[flow_sensor]['ges'] = offline_data['flow_data'][str_sensor]['ges']
                
                self.data_file.unlink()
                logging.info("Offline data loaded and removed")
                
        except Exception as e:
            logging.error(f"Error loading offline data: {e}")
    
    # System monitoring methods
    def _measure_temp(self) -> str:
        """Measure CPU temperature"""
        try:
            result = os.popen("vcgencmd measure_temp").readline()
            temp = result.replace("temp=", "").replace("'C\n", "")
            return temp
        except Exception as e:
            logging.error(f"Error measuring temperature: {e}")
            return "0.0"
    
    def _get_uptime(self) -> timedelta:
        """Get system uptime"""
        return datetime.now() - datetime.fromtimestamp(psutil.boot_time())
    
    def update_lcd(self):
        """Update LCD display"""
        if not LCD_AVAILABLE:
            return
        
        try:
            display_options = [
                f"Cloud: {self.mqtt_client.connected_flag if self.mqtt_client else False}",
                f"Modbus: {self.modbus_client.connected_flag if self.modbus_client else False}",
                f"Power: {self.power_sensor.connected if self.power_sensor else False}",
                f"Uptime: {str(self._get_uptime()).split('.')[0]}",
                f"CPU: {psutil.cpu_percent():.1f}%"
            ]
            
            # Show flow data if available
            if self.flow_data:
                for pin, data in self.flow_data.items():
                    display_options.append(f"Flow {pin}: {data['ges']:.2f}L")
            
            if self.lcd_count >= len(display_options):
                self.lcd_count = 0
            
            g_lcd.content_display(display_options[self.lcd_count])
            self.lcd_count += 1
            
        except Exception as e:
            logging.error(f"Error updating LCD: {e}")
    
    def check_plc_status(self):
        """Check PLC status via API"""
        try:
            if not self.settings.get('general', {}).get('apikey'):
                return
            
            response, content = self._make_api_request("api/get/data?elm=STATE")
            if response and content:
                data = json.loads(content)
                self.data['plc_state'] = (data.get("SYSINFO", {}).get("STATE")) == "RUN"
                
        except Exception as e:
            logging.error(f"Error checking PLC status: {e}")
    
    def check_modbus_connection(self):
        """Check and maintain Modbus connection"""
        try:
            if self.modbus_client:
                if not self.modbus_client.connected_flag:
                    if self.modbus_client.connect():
                        self.modbus_client.connected_flag = True
                        logging.info("Modbus connection restored")
                    else:
                        self.modbus_client.connected_flag = False
        except Exception as e:
            logging.error(f"Error checking Modbus connection: {e}")
            if self.modbus_client:
                self.modbus_client.connected_flag = False
    
    def _make_api_request(self, endpoint: str):
        """Make API request to PLC"""
        try:
            headers = {
                'Authorization': f"Bearer {self.settings['general']['apikey']}",
                'Connection': 'close'
            }
            
            h = httplib2.Http(".cache", disable_ssl_certificate_validation=True)
            url = f"https://{self.settings['general']['ip']}/{endpoint}"
            
            return h.request(url, method="GET", headers=headers)
            
        except Exception as e:
            logging.error(f"Error making API request: {e}")
            return None, None
    
    def read_temperature_sensors(self):
        """Read 1-Wire temperature sensors"""
        if not SENSORS_AVAILABLE:
            return
        
        try:
            i = 0
            for sensor in temp.get_available_sensors():
                sp = temp.slave_prefix(sensor[0])
                sensor_path = temp.path.join(
                    temp.BASE_DIRECTORY, 
                    sp + sensor[1], 
                    temp.SLAVE_FILE
                )
                i += 1
                self.sensor_data[f'temp{i}'] = temp.read_temp(sensor_path)
                
        except Exception as e:
            logging.error(f"Error reading temperature sensors: {e}")
    
    def process_modbus_data(self):
        """Process Modbus data from PLC"""
        if not (self.modbus_client and self.modbus_client.connected_flag):
            self.data['plc_connect'] = "false"
            return
        
        try:
            modbus_data_config = self.settings.get('modbus_data', [])
            
            for config in modbus_data_config:
                if not all(key in config for key in ['prefix', 'address', 'length', 'method']):
                    continue
                
                prefix = config['prefix']
                address = int(config['address'])
                length = int(config['length'])
                method = int(config['method'])
                
                try:
                    if method == 1:  # Read coils
                        result = self.modbus_client.read_coils(address, length, slave=1)
                    elif method == 2:  # Read discrete inputs
                        result = self.modbus_client.read_discrete_inputs(address, length, slave=1)
                    else:
                        continue
                    
                    if not result.isError():
                        for i, bit in enumerate(result.bits[:length], 1):
                            self.data[f"{prefix}{i}"] = int(bit)
                            
                except ModbusException as e:
                    logging.error(f"Modbus error for {prefix}: {e}")
            
            self.data['plc_connect'] = "true"
            
            # Check for data changes and publish if needed
            data_changes = set(self.data.items()) - set(self.data_previous.items())
            if data_changes:
                self.publish_data()
            
            self.data_previous = copy.deepcopy(self.data)
            
            # Update counters
            self._update_counters()
            
        except Exception as e:
            logging.error(f"Error processing Modbus data: {e}")
            self.data['plc_connect'] = "false"
    
    def _update_counters(self):
        """Update counter values based on pin states"""
        counter_config = self.settings.get('pin_settings', {}).get('counter', [])
        
        for counter in counter_config:
            if not isinstance(counter, dict):
                continue
                
            name = counter.get('name')
            pin = counter.get('pin')
            
            if not (name and pin):
                continue
            
            if name not in self.counters:
                self.counters[name] = 0
            
            if pin in self.data and self.data[pin]:
                self.counters[name] += 1/3600  # Increment by hours
    
    def process_flow_sensors(self):
        """Process flow sensor data"""
        if not self.flow_data:
            return
        
        try:
            self.start_counter = 1
            time.sleep(1)
            self.start_counter = 0
            
            for pin, data in self.flow_data.items():
                # Calculate flow rate
                data['rate'] = data['count'] / data['ppl']
                data['ges'] += data['rate']
                data['count'] = 0
                
                logging.debug(f"Flow sensor {pin}: {data['ges']:.3f} Liter total")
                
        except Exception as e:
            logging.error(f"Error processing flow sensors: {e}")
    
    def run(self):
        """Main run loop"""
        self.running = True
        logging.info("IoT Control System started")
        
        try:
            # Load any offline data
            self.load_offline_data()
            
            while self.running:
                try:
                    # Process Modbus data
                    self.process_modbus_data()
                    
                    # Process flow sensors if available
                    if self.flow_data:
                        self.process_flow_sensors()
                    
                    # Read temperature sensors
                    self.read_temperature_sensors()
                    
                    # Run scheduled tasks
                    schedule.run_pending()
                    
                    # Sleep between cycles
                    time.sleep(1)
                    
                except KeyboardInterrupt:
                    logging.info("Keyboard interrupt received")
                    break
                except Exception as e:
                    logging.error(f"Error in main loop: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            logging.error(f"Fatal error in run loop: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logging.info("Cleaning up resources...")
        
        self.running = False
        
        # Stop MQTT
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        # Close Modbus connection
        if self.modbus_client:
            self.modbus_client.close()
        
        # Disconnect power sensor
        if self.power_sensor:
            self.power_sensor.disconnect()
        
        # Stop ESP32 handler
        if self.esp32_handler:
            self.esp32_handler.stop()
        
        # Clean up GPIO
        if self.flow_data:
            for pin in self.flow_data:
                try:
                    GPIO.remove_event_detect(pin)
                except:
                    pass
            GPIO.cleanup()
        
        # Clear LCD
        if LCD_AVAILABLE:
            try:
                g_lcd.clear_lcd()
            except:
                pass
        
        logging.info("Cleanup completed")


def main():
    """Main entry point"""
    try:
        # Parse command line arguments if needed
        settings_file = '/boot/svs/settings.json'
        if len(sys.argv) > 1:
            settings_file = sys.argv[1]
        
        # Create and run the system
        system = IoTControlSystem(settings_file)
        system.run()
        
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
