import pandas as pd

def baca_data_excel(nama_file):
    df = pd.read_excel(nama_file)
    return df

def tulis_data_excel(nama_file, df):
    df.to_excel(nama_file, index=False)

def main():
    nama_file = 'produk.xlsx'
    produk = baca_data_excel(nama_file)

    print("Data Produk")
    print(produk)

    tambah_produk = input("Tambah produk baru (y/t)?: ")
    if tambah_produk.lower() == 'y':
        nama_produk = input("Nama produk: ")
        harga_produk = int(input("Harga produk (IDR): "))
        stok_produk = input("Stok produk: ")

        produk_baru = {
            'Nama': nama_produk,
            'Harga': harga_produk,
            'Stok': stok_produk
        }
        produk = produk.append(produk_baru, ignore_index=True)

        tulis_data_excel(nama_file, produk)
        print("Produk berhasil ditambahkan dan disimpan")

if __name__ == "__main__":
    main()