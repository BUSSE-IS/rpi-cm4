#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import minimalmodbus
import time

class SDM230Reader:
    """Vereinfachte Klasse zum Auslesen eines Eastron SDM230 über Modbus RS485."""
    
    # Register für die benötigten Werte
    REGISTER_VOLTAGE = 0x0000           # Spannung in V
    REGISTER_CURRENT = 0x0006           # Strom in A
    REGISTER_TOTAL_ACTIVE_ENERGY = 0x0156  # Gesamte Wirkenergie in kWh
    
    def __init__(self, port='/dev/ttyAMA2', device_address=1, baudrate=19200):
        """Initialisiere die Verbindung zum SDM230.
        
        Args:
            port (str): Serieller Port (z.B. '/dev/ttyAMA5')
            device_address (int): Modbus-Geräteadresse des SDM230
            baudrate (int): Baudrate
        """
        self.instrument = minimalmodbus.Instrument(port, device_address)
        self.instrument.serial.baudrate = baudrate
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN  # Keine Parität
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        
        # Modbus RTU Konfiguration
        self.instrument.mode = minimalmodbus.MODE_RTU
        self.instrument.close_port_after_each_call = True

    def read_voltage(self):
        """Spannung in Volt auslesen.
        
        Returns:
            float: Spannung in V
        """
        try:
            return self.instrument.read_float(self.REGISTER_VOLTAGE, functioncode=4)
        except Exception as e:
            print(f"Fehler beim Lesen der Spannung: {e}")
            return None

    def read_current(self):
        """Strom in Ampere auslesen.
        
        Returns:
            float: Strom in A
        """
        try:
            return self.instrument.read_float(self.REGISTER_CURRENT, functioncode=4)
        except Exception as e:
            print(f"Fehler beim Lesen des Stroms: {e}")
            return None

    def read_energy(self):
        """Gesamte Wirkenergie in kWh auslesen.
        
        Returns:
            float: Wirkenergie in kWh
        """
        try:
            return self.instrument.read_float(self.REGISTER_TOTAL_ACTIVE_ENERGY, functioncode=4)
        except Exception as e:
            print(f"Fehler beim Lesen der Wirkenergie: {e}")
            return None

    def read_all(self):
        """Alle drei benötigten Werte auslesen.
        
        Returns:
            dict: Dictionary mit den Werten für 'voltage', 'current' und 'energy'
        """
        return {
            'voltage': self.read_voltage(),
            'current': self.read_current(),
            'energy': self.read_energy()
        }

# Beispiel zur Verwendung des Codes:
if __name__ == "__main__":
    # Nur zur Demonstration - dieser Block wird nicht ausgeführt, wenn das Skript importiert wird
    reader = SDM230Reader(port='/dev/ttyAMA2')
    
    # Einzelne Werte auslesen
    voltage = reader.read_voltage()
    current = reader.read_current()
    energy = reader.read_energy()
    
    print(f"Spannung: {voltage:.1f} V")
    print(f"Strom: {current:.3f} A")
    print(f"Gesamte Wirkenergie: {energy:.2f} kWh")
    
    # Oder alle Werte auf einmal
    values = reader.read_all()
    print(f"Alle Werte: {values}")
