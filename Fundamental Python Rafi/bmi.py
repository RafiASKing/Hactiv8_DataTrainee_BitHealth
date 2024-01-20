def hitung_bmi(berat, tinggi):
    tinggi_m = tinggi / 100
    bmi = berat / (tinggi_m ** 2)
    return bmi

def kategori_bmi(bmi):
    if bmi < 18.5:
        return "Kurus"
    elif 18.5 <= bmi < 24.9:
        return "Normal"
    elif 25 <= bmi < 29.9:
        return "Gemuk"
    else:
        return "Obesitas"
    
berat = float(input("masukkan berat badan (kg): "))
tinggi = float(input("Masukkan tinggi badan (cm): "))

bmi = hitung_bmi(berat, tinggi)
kategori = kategori_bmi(bmi)

print(f"BMI Anda adalah {bmi:.2f} ({kategori}).")