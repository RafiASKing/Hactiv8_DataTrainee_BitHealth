teks = """ini adalah contoh teks.
Teks ini digunakan untuk latihan pencarian kata."""

kata_cari = input("Masukan kata yang ingin Anda cari: ")

if kata_cari in teks:
    print("Kata", kata_cari, "ada dalam teks")
else:
    print("Kata", kata_cari, "tidak ada dalam teks")