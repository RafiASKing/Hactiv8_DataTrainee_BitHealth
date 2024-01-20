import sqlite3

# Fungsi untuk membaca data dari table:
def baca_data_sqlite(database_file):
    connetion = sqlite3.connect(database_file)
    cursor = connetion.cursor()
    cursor.execute("SELECT * FROM produk")
    data = cursor.fetchall()
    connetion.close()
    return data

# Fungsi untuk menambahkan data ke dalam table:
def tulis_data_sqlite(database_file, name_produk, harga, stok):
    connetion = sqlite3.connect(database_file)
    cursor = connetion.cursor()
    cursor.executemany("INSERT INTO produk (name_produk, harga, stok) VALUES (?, ?, ?)", (nama_produk, harga, stok))
    connetion.commit()
    connetion.close()

def main():
    database_file = "produk.db" 
    produk = baca_data_sqlite(database_file)
    
    print("Data Produk:")
    for item in produk:
        print(f"{item[1]} : (item[2]) IDR, Stok: {item[3]}")

    tambah_produk = input("Tambah produk? (y/n): ")
    if tambah_produk.lower() == "y":
        nama_produk = input("Nama produk: ")
        harga_produk = int(input("Harga produk (IDR): "))
        stok_produk = int(input("Stok produk: "))

        tambah_data_sqlite(database_file, nama_file, harga_produk, stok_produk)
        print("Produk berhasil ditambahkan dan disimpan")

if __name__ == "__main__":
    main()