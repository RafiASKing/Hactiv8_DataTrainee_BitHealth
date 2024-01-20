# main.py
import my_module

# Menggunakan fungsi dari my_module
message = my_module.greet("Alice")
print(message)

# Menggunakan variabel dari my_module
radius = 5
area = my_module.pi * my_module.square(radius)
print(f"Luas ingkaran dengan radius {radius} adalah {area:.2f}")
