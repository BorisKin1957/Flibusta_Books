# hexdump.py — вывод hex-дампа последних 100 байт файла

filename = "d.fb2.inp"

with open(filename, "rb") as f:
    data = f.read()

# Берём последние ~100 байт для анализа конца
chunk = data[-100:]

print("Последние байты файла (hex dump):")
for i in range(0, len(chunk), 16):
    line = chunk[i:i+16]
    hex_part = " ".join(f"{b:02x}" for b in line)
    str_part = "".join(chr(b) if 32 <= b < 127 else "." for b in line)
    print(f"{i:08x}  {hex_part:<47}  |{str_part}|")