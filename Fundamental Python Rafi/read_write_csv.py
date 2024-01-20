import csv

def baca_data_csv(nama_file):
    data = []
    with open(nama_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data
    
def tulis_data_csv(nama_file, data):
    with open(nama_file, mode='w', newline='') as file:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    nama_file = 'produk.csv'
    produk = baca_data_csv(nama_file)

    print("Data Produk:")
    for item in produk:
        print(f"{item['Nama Produk']}: {item['Harga']} IDR, Stok: {item['Stok']}")

    tambah_produk = input("Tambah produk baru? (y/n) ")
    if tambah_produk.lower() == 'y':
        nama_produk = input("Nama produk: ")
        harga_produk = input("Harga produk (IDR): ")
        stok_produk = input("Stok produk: ")

        produk_baru = {'Nama Produk': nama_produk, 'Harga': harga_produk, 'Stok': stok_produk}
        produk.append(produk_baru)

        tulis_data_csv(nama_file, produk)
        print("Produk berhasil ditambahkan dan disimpan.")

if __name__ == '__main__':
    main()