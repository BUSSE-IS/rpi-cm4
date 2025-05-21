import minimalmodbus
import serial
import time

# Modbus-Parameter
PORT = "/dev/ttyUSB0"       # Anpassen an deinen Anschluss
SLAVE_ADDRESS = 1           # Modbus-Adresse deines SDM230
BAUDRATE = 9600             # Standardwert für SDM230
TIMEOUT = 1

# Registerdefinitionen für SDM230 (laut Doku)
SDM230_REGISTERS = {
    "Spannung_L1": 0,       # Registeradresse für Spannung L-N (V)
    "Strom": 6,             # Registeradresse für Strom (A)
    "Leistung": 12          # Registeradresse für Wirkleistung (W)
}

def read_float(instrument, register, decimals=2):
    """Liest einen 32-Bit-IEEE754-Float-Wert aus dem Modbus-Gerät."""
    try:
        value = instrument.read_float(registeraddress=register, functioncode=4, number_of_registers=2)
        return round(value, decimals)
    except (IOError, ValueError) as e:
        print(f"Fehler beim Lesen von Register {register}: {e}")
        return None

def main():
    # Initialisierung
    instrument = minimalmodbus.Instrument(PORT, SLAVE_ADDRESS)
    instrument.serial.baudrate = BAUDRATE
    instrument.serial.timeout = TIMEOUT
    instrument.mode = minimalmodbus.MODE_RTU
    instrument.clear_buffers_before_each_transaction = True

    # Warten, falls USB-Adapter neu initialisiert wurde
    time.sleep(1)

    print("Lese Werte vom SDM230...")

    # Beispiel: Spannung an L1 auslesen
    spannung = read_float(instrument, SDM230_REGISTERS["Spannung_L1"])
    print(f"Spannung L1: {spannung} V")

    # Beispiel: Strom auslesen
    strom = read_float(instrument, SDM230_REGISTERS["Strom"])
    print(f"Strom: {strom} A")

    # Beispiel: Wirkleistung auslesen
    leistung = read_float(instrument, SDM230_REGISTERS["Leistung"])
    print(f"Leistung: {leistung} W")

if __name__ == "__main__":
    main()
