# Daftar Belanja

shopping_list = []

def tambah_item(item):
    shopping_list.append(item)
    print(f"{item} telah ditambahkan ke daftar belanja.")

def hapus_item(item):
    if item in shopping_list:
        shopping_list.remove(item)
        print(f"{item} telah dihapus dari daftar belanja.")
    else:
        print(f"{item} tidak ditemukan dalam daftar belanja.")

def tampilkan_daftar():
    print("Daftar Belanja:")
    for item in shopping_list:
        print("-", item)


while True:
    print("Apa yang ingin Anda lakukan?")
    print("1. Tambah item ke dafter belanja")
    print("2. Hapus item dari dafter belanja")
    print("3. Tampilkan daftar belanja")
    print("4. Keluar")

    pilihan = input("Masukkan pilihan tindakan (1/2/3/4): ")

    if pilihan == "1":
        item = input("Masukkan item yang ingin Anda tambahkan: ")
        tambah_item(item)
    elif pilihan == "2":
        item = input("Masukkan item yang ingin dihapus: ")
        hapus_item(item)
    elif pilihan == "3":
        tampilkan_daftar()
    elif pilihan == "4":
        print("Keluar dari daftar belanja")
        break
    else:
        print("Pilihan tidak valid.")