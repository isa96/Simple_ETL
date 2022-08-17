#!/usr/bin/env python
# coding: utf-8

# # Pengantar

# ### Extract, Transform dan Load (ETL) 
# merupakan kumpulan proses untuk "memindahkan" data dari satu tempat ke tempat lain. Tempat yang dimaksud adalah dari sumber data (bisa berupa database aplikasi, file, logs, database dari 3rd party, dan lainnya) ke data warehouse.

# ### Apa itu data warehouse?
# Singkatnya, data warehouse merupakan database yang berisi data-data (tabel-tabel) yang sudah siap untuk dilakukan analisis oleh Data Analyst maupun Data Scientist.

# ### Project yang Akan Dikerjakan 
# Pada proyek kali ini, Anda diminta untuk mengolah data pendaftar hacktahon yang diselenggarkan oleh DQLab bernama DQThon.
# 
# Dataset ini terdiri dari 5000 baris data (5000 pendaftar) dengan format CSV (Comma-separated values) dan memiliki beberapa kolom diantaranya:
# 
# 1.  participant_id: ID dari peserta/partisipan hackathon. Kolom ini bersifat unique sehingga antar peserta pasti memiliki ID yang berbeda
# 2.  first_name: nama depan peserta
# 3.  last_name: nama belakang peserta
# 4.  birth_date: tanggal lahir peserta
# 5.  address: alamat tempat tinggal peserta
# 6.  phone_number: nomor hp/telfon peserta
# 7.  country: negara asal peserta
# 8.  institute: institusi peserta saat ini, bisa berupa nama perusahaan maupun nama universitas
# 9.  occupation: pekerjaan peserta saat ini
# 10. register_time: waktu peserta melakukan pendaftaran hackathon dalam second
# 
# Namun pada proyek ini nantinya Anda diminta untuk menghasilkan beberapa kolom dengan memanfaatkan kolom-kolom yang ada, sehingga akhir dari proyek ini berupa hasil transformasi data dengan beberapa kolom baru selain dari 10 kolom diatas.

# ### Extract
# Extract merupakan proses meng-ekstraksi data dari sumber, sumber data ini bisa berupa relational data (SQL) atau tabel, nonrelational (NoSQL) maupun yang lainnya.

# In[211]:


import pandas as pd
df_participant = pd.read_csv('dqthon-participants.csv', names = ["participant_id", "first_name", "last_name", "birth_date", "address", "phone_number", "country", "institute", "occupation", "register_time"])


# In[212]:


df_participant.head()


# In[213]:


df_participant.drop([0], inplace=True)
df_participant.reset_index(drop=True, inplace=True)
df_participant


# ### Transform
# Transform merupakan proses melakukan transformasi data, atau perubahan terhadap data. Umumnya seperti:
# 
# 1. Merubah nilai dari suatu kolom ke nilai baru,
# 2. Menciptakan kolom baru dengan memanfaatkan kolom lain,
# 3. Transpose baris menjadi kolom (atau sebaliknya),
# 4. Merubah format data ke bentuk yang lebih standar (contohnya kolom date, maupun datetime yang biasanya memiliki nilai yang
#    tidak standar maupun nomor HP yang biasanya memiliki nilai yang tidak sesuai format standarnya), dan lainnya. 
# 
# 

# #### Transform Bagian I - Kode Pos
# Ada permintaan datang dari tim Logistik bahwa mereka membutuhkan kode pos dari peserta agar pengiriman piala lebih mudah dan cepat sampai. Maka dari itu buatlah kolom baru bernama postal_code yang memuat informasi mengenai kode pos yang diambil dari alamat peserta (kolom address).
# 
# Diketahui bahwa kode pos berada di paling akhir dari alamat tersebut.
# 
# Note:
# Jika regex yang dimasukkan tidak bisa menangkap pattern dari value kolom address maka akan menghasilkan NaN.

# In[214]:


df_participant['postal_code'] = df_participant['address'].str.extract(r'(\d+)$') #Masukkan regex Anda didalam fungsi extract
df_participant


# #### Transform Bagian II - Kota
# Selain kode pos, mereka juga membutuhkan kota dari peserta.
# 
# Untuk menyediakan informasi tersebut, buatlah kolom baru bernama city yang didapat dari kolom address. Diasumsikan bahwa kota merupakan sekumpulan karakter yang terdapat setelah nomor jalan diikuti dengan \n (newline character) atau dalam bahasa lainnya yaitu enter.

# In[215]:


#df_participant['city'] = df_participant['address'].str.extract( r'(?:\d+)\n+(\w+)') #Masukkan regex Anda didalam fungsi extract
df_participant['city'] = df_participant['address'].str.extract( r'(?:\\n+(\w+)|\b\-\w+)')
df_participant


# #### Transform Bagian III - Github
# Salah satu parameter untuk mengetahui proyek apa saja yang pernah dikerjakan oleh peserta yaitu dari git repository mereka.
# 
# Pada kasus ini kita menggunakan profil github sebagai parameter nya. Tugas Anda yaitu membuat kolom baru bernama github_profile yang merupakan link profil github dari peserta.
# 
# Diketahui bahwa profil github mereka merupakan gabungan dari first_name dan last_name yang sudah di-lowercase. 

# In[216]:


df_participant['github_profile'] = 'https://github.com/' + df_participant['first_name'].str.lower() + df_participant['last_name'].str.lower()


# #### Transform Bagian IV - Nomor Handphone
# Jika kita lihat kembali, ternyata nomor handphone yang ada pada data csv kita memiliki format yang berbeda-beda. Maka dari itu, kita perlu untuk melakukan cleansing pada data nomor handphone agar memiliki format yang sama. Anda sebagai Data Engineer diberi privilege untuk menentukan format nomor handphone yang benar. Pada kasus ini mari kita samakan format nya dengan aturan:
# 
# 1. Jika awalan nomor HP berupa angka 62 atau +62 yang merupakan kode telepon Indonesia, maka diterjemahkan ke 0.
# 2. Tidak ada tanda baca seperti kurung buka, kurung tutup, strip⟶ ()-
# 3. Tidak ada spasi pada nomor HP Nama kolom untuk menyimpan hasil cleansing pada nomor HP yaitu cleaned_phone_number
# 

# In[217]:


#Masukkan regex anda pada parameter pertama dari fungsi replace
df_participant['cleaned_phone_number'] = df_participant['phone_number'].str.replace(r'^(\+62|62)', '0')
df_participant['cleaned_phone_number'] = df_participant['cleaned_phone_number'].str.replace(r'[()-]', '')
df_participant['cleaned_phone_number'] = df_participant['cleaned_phone_number'].str.replace(r'\s+', '')


# #### Transform Bagian V - Nama Tim
# Dataset saat ini belum memuat nama tim, dan rupanya dari tim Data Analyst membutuhkan informasi terkait nama tim dari masing-masing peserta.
# 
# Diketahui bahwa nama tim merupakan gabungan nilai dari kolom first_name, last_name, country dan institute.
# 
# Tugas Anda yakni buatlah kolom baru dengan nama team_name yang memuat informasi nama tim dari peserta.

# In[218]:


def func(col):
    abbrev_name = "%s%s"%(col['first_name'][0],col['last_name'][0]) #Singkatan dari Nama Depan dan Nama Belakang dengan mengambil huruf pertama
    country = col['country']
    abbrev_institute = '%s'%(''.join(list(map(lambda word: word[0], col['institute'].split())))) #Singkatan dari value di kolom institute
    return "%s-%s-%s"%(abbrev_name,country,abbrev_institute)

df_participant['team_name'] = df_participant.apply(func, axis=1)


# #### Transform Bagian VI - Email
# Setelah dilihat kembali dari data peserta yang dimiliki, ternyata ada satu informasi yang penting namun belum tersedia, yaitu email.
# 
# Anda sebagai Data Engineer diminta untuk menyediakan informasi email dari peserta dengan aturan bahwa format email sebagai berikut:
# 
# Format email:
# xxyy@aa.bb.[ac/com].[cc]
# 
# Keterangan:
# xx -> nama depan (first_name) dalam lowercase
# yy -> nama belakang (last_name) dalam lowercase
# aa -> nama institusi
# 
# Untuk nilai bb, dan cc mengikuti nilai dari aa. Aturannya:
# - Jika institusi nya merupakan Universitas, maka
#   bb -> gabungan dari huruf pertama pada setiap kata dari nama Universitas dalam lowercase
#   Kemudian, diikuti dengan .ac yang menandakan akademi/institusi belajar dan diikuti dengan pattern cc
# - Jika institusi bukan merupakan Universitas, maka
#   bb -> gabungan dari huruf pertama pada setiap kata dari nama Universitas dalam lowercase
#   Kemudian, diikuti dengan .com. Perlu diketahui bahwa pattern cc tidak berlaku pada kondisi ini
# 
# cc -> merupakan negara asal peserta, adapun aturannya:
# - Jika banyaknya kata pada negara tersebut lebih dari 1 maka ambil singkatan dari negara tersebut dalam lowercase
# - Namun, jika banyaknya kata hanya 1 maka ambil 3 huruf terdepan dari negara tersebut dalam lowercase
# 
# Contoh:
#   Nama depan: Citra
#   Nama belakang: Nurdiyanti
#   Institusi: UD Prakasa Mandasari
#   Negara: Georgia
#   Maka,Email nya: citranurdiyanti@upm.geo
#   
#   -----------------------------------
#   Nama depan: Aris
#   Nama belakang: Setiawan
#   Institusi: Universitas Diponegoro
#   Negara: Korea Utara
#   Maka, Email nya: arissetiawan@ud.ac.ku

# In[219]:


def func(col):
    first_name_lower = col['first_name'].lower()
    last_name_lower = col['last_name'].lower()
    institute = ''.join(list(map(lambda word: word[0], col['institute'].lower().split()))) #Singkatan dari nama perusahaan dalam lowercase
    if 'Universitas' in col['institute']:
        if len(col['country'].split()) > 1: #Kondisi untuk mengecek apakah jumlah kata dari country lebih dari 1
            country = ''.join(list(map(lambda word: word[0], col['country'].lower().split())))
        else:
            country = col['country'][:3].lower()
        return "%s%s@%s.ac.%s"%(first_name_lower,last_name_lower,institute,country)

    return "%s%s@%s.com"%(first_name_lower,last_name_lower,institute)

df_participant['email'] = df_participant.apply(func, axis=1)


# In[220]:


df_participant


# #### Transform Bagian VII - Tanggal Lahir
# MySQL merupakan salah satu database yang sangat populer dan digunakan untuk menyimpan data berupa tabel, termasuk data hasil pengolahan yang sudah kita lakukan ini nantinya bisa dimasukkan ke MySQL.
# 
# Meskipun begitu, ada suatu aturan dari MySQL terkait format tanggal yang bisa mereka terima yaitu YYYY-MM-DD dengan keterangan:
# 
# - YYYY: 4 digit yang menandakan tahun
# - MM: 2 digit yang menandakan bulan
# - DD: 2 digit yang menandakan tanggal
# 
# Contohnya yaitu: 2021-04-07
# 
# Jika kita lihat kembali pada kolom tanggal lahir terlihat bahwa nilai nya belum sesuai dengan format DATE dari MySQL.
# Oleh karena itu, lakukanlah formatting terhadap kolom birth_date menjadi YYYY-MM-DD dan simpan di kolom yang sama.

# In[221]:


df_participant['birth_date'] = pd.to_datetime(df_participant['birth_date'], format='%d %b %Y')
df_participant


# #### Transform Bagian VIII - Tanggal Daftar Kompetisi
# Selain punya aturan mengenai format DATE, MySQL juga memberi aturan pada data yang bertipe DATETIME yaitu YYYY-MM-DD HH:mm:ss dengan keterangan:
# 
# - YYYY: 4 digit yang menandakan tahun
# - MM: 2 digit yang menandakan bulan
# - DD: 2 digit yang menandakan tanggal
# - HH: 2 digit yang menandakan jam
# - mm: 2 digit yang menandakan menit
# - ss: 2 digit yang menandakan detik
# 
# Contohnya yaitu: 2021-04-07 15:10:55
# 
# Karena data kita mengenai waktu registrasi peserta (register_time) belum sesuai format yang seharusnya.
# 
# Maka dari itu, tugas Anda yaitu untuk merubah register_time ke format DATETIME sesuai dengan aturan dari MySQL.
# 
# Simpanlah hasil tersebut ke kolom register_at.

# In[222]:


df_participant['register_at'] = pd.to_datetime(df_participant['register_time'], unit='s')


# In[223]:


df_participant.reset_index(drop=True)


# ### Kesimpulan
# Dengan begitu, tibalah kita di penghujung dari chapter bagian Transform ini.
# 
# Jika dilihat kembali, dataset Anda saat ini sudah berbeda dengan saat proses extract sebelumnya. Ada beberapa kolom tambahan yang memanfaatkan nilai kolom lain.
# 
# Dataset Anda saat ini memuat kolom:
# 
# - participant_id: ID dari peserta/partisipan hackathon. Kolom ini bersifat unique sehingga antar peserta pasti memiliki ID
# yang berbeda
# - first_name: nama depan peserta
# - last_name: nama belakang peserta
# - birth_date: tanggal lahir peserta (sudah diformat ke YYYY-MM-DD)
# - address: alamat tempat tinggal peserta
# - phone_number: nomor hp/telfon peserta
# - country: negara asal peserta
# - institute: institusi peserta saat ini, bisa berupa nama perusahaan maupun nama universitas
# - occupation: pekerjaan peserta saat ini
# - register_time: waktu peserta melakukan pendaftaran hackathon dalam second
# - team_name: nama tim peserta (gabungan dari nama depan, nama belakang, negara dan institusi)
# - postal_code: kode pos alamat peserta (diambil dari kolom alamat)
# - city: kota peserta (diambil dari kolom alamat)
# - github_profile: link profil github peserta (gabungan dari nama depan, dan nama belakang)
# - email: alamat email peserta (gabungan dari nama depan, nama belakang, institusi dan negara)
# - cleaned_phone_number: nomor hp/telfon peserta (sudah lebih sesuai dengan format nomor telfon)
# - register_at: tanggal dan waktu peserta melakukan pendaftaran (sudah dalam format DATETIME)
# 

# ### Load
# Pada bagian load ini, data yang sudah ditransformasi sedemikian rupa sehingga sesuai dengan kebutuhan tim Analyst dimasukkan kembali ke database yaitu Data Warehouse (DWH). Biasanya, dilakukan pendefinisian skema database terlebih dahulu seperti:
# 
# - Nama kolom
# - Tipe kolom
# - Apakah primary key, unique key, index atau bukan
# - Panjang kolomnya
# 
# Karena umumnya Data Warehouse merupakan database yang terstruktur sehingga mereka memerlukan skema sebelum data nya dimasukkan.
# 
# Pandas sudah menyediakan fungsi untuk memasukkan data ke database yaitu to_sql() .

# In[224]:


from sqlalchemy import create_engine


# In[225]:


engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",
                                                                           pw="roger123",
                                                                           db="dqthon"))


# In[226]:


df_participant.to_sql('participant', con = engine, if_exists = 'append', chunksize = 1000, index= False)


# In[ ]:




