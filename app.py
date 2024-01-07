from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dateutil.relativedelta import *
import zipfile, sys, os, psycopg2, wget
import datetime

spark = SparkSession \
        .builder \
        .appName("App") \
        .getOrCreate()

csv_path = "csv_files/"
zip_path = "zip_files/"
curated_path = "curated_files/"

use_date = datetime.datetime.now() + relativedelta(months=-1)
ano_mes = use_date.strftime("%Y%m")

# Function to extract data from the CNES server in zip format
def extract_zip(period):
    
    ftp_path = f"ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_{ano_mes}.ZIP"
    
    # Check if file does not already exist to start FTP server download
    if os.path.exists(zip_path + f"BASE_DE_DADOS_CNES_{ano_mes}.ZIP") == False: 
        try:
            print(f"Starting data extraction for period {ano_mes}")
            wget.download(ftp_path, zip_path) # Access FTP server to download file
            print(f"Data for period {ano_mes} extracted")
        except:
            print("The error", sys.exc_info()[0], "occurred.")
    else:
        print(f"The zip file for {ano_mes} already exists")

# Function to extract data from a zipped file
def extract_csv(period):
    print("Starting extraction of CSV files")
    final_path = f"{csv_path}{period}"
    
    # Check if the destination folder does not exist to start the process
    if os.path.exists(final_path) == False:
        try:
            os.chdir(zip_path)
            for file in os.listdir(zip_path):
                if zipfile.is_zipfile(file): 
                    with zipfile.ZipFile(file) as item: 
                       item.extractall(final_path)
                    os.remove(file)
        except:
            print("The error", sys.exc_info()[0], "occurred.")     
    else:
        print(f"The destination folder {ano_mes} already exists")       

# Function to load CSV files as spark Dataframes
def get_csv(file_name, period):
    
    file_path = f"{csv_path}{period}/{file_name}{period}.csv"
    
    try:
        df = spark.read\
                .option("header", "true")\
                .option("delimiter", ";")\
                .csv(f"{file_path}")
        return df
    except:
        print("The error", sys.exc_info()[0], "occurred.")

# Function to write spark Dataframes as curated CSV files
def write_curated_file(df, file_name, ano_mes):
    file_path = f'{curated_path}{file_name}_{ano_mes}'

    try:
        df.coalesce(1)\
                .write\
                .mode("overwrite")\
                .option("header","True")\
                .csv(f"{file_path}")

        for partition in os.listdir(f"{file_path}"):
            if partition.startswith("part-"):
                old_name = os.path.join(f"{file_path}", partition)
                new_name = os.path.join(f"{file_path}", f"{file_name}_{ano_mes}.csv")
                os.rename(old_name, new_name)
        print(f"DataFrame {file_name} written to Curated layer")
    
    except:
        print("The error", sys.exc_info()[0], "occurred.")

# Function to load curated CSV files into PostgreSQL tables
def update_table(table_name, file_name, ano_mes):

    try:
        conn = psycopg2.connect(database="postgres",
                                user='postgres', password='', 
                                host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()

        new_file = f'{curated_path}{file_name}_{ano_mes}/{file_name}_{ano_mes}.csv'

        truncate = f'TRUNCATE TABLE {table_name}'
        cursor.execute(truncate)
        
        update = f'''COPY {table_name} FROM '{new_file}' DELIMITER ',' CSV HEADER;'''
        cursor.execute(update)
        print(f"Table {table_name} updated with data from {ano_mes}")
    
    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection closed")

if __name__ == "__main__":

    # Starting with the extraction of the .zip file from the CNES server
    extract_zip(ano_mes)

    # Extraction of the csv files from the compressed file
    extract_csv(ano_mes)

    # Creating spark DataFrames from the csv files to perform transformations
    tbCargaHorariaSus = get_csv('tbCargaHorariaSus', ano_mes)
    rlEstabServClass = get_csv('rlEstabServClass', ano_mes)
    tbAtividadeProfissional = get_csv('tbAtividadeProfissional', ano_mes)
    tbClassificacaoServico = get_csv('tbClassificacaoServico', ano_mes)
    tbDadosProfissionalSus = get_csv('tbDadosProfissionalSus', ano_mes)
    tbEstabelecimento = get_csv('tbEstabelecimento', ano_mes)
    tbMunicipio = get_csv('tbMunicipio', ano_mes)

    # Transformations of the csv files into two final tables
    cond_sp = [tbEstabelecimento.CO_MUNICIPIO_GESTOR == tbMunicipio.CO_MUNICIPIO,
           tbEstabelecimento.CO_ESTADO_GESTOR == 35] #SP

    cond_serv = [rlEstabServClass.CO_SERVICO == tbClassificacaoServico.CO_SERVICO_ESPECIALIZADO,
        rlEstabServClass.CO_CLASSIFICACAO == tbClassificacaoServico.CO_CLASSIFICACAO_SERVICO]

    estab_munic = tbEstabelecimento.join(tbMunicipio, cond_sp)

    
    df_serv_joined = rlEstabServClass\
            .join(tbClassificacaoServico, cond_serv)\
            .join(estab_munic, rlEstabServClass.CO_UNIDADE == estab_munic.CO_UNIDADE)\
            .select(rlEstabServClass.CO_UNIDADE,
                estab_munic.NO_MUNICIPIO,
                estab_munic.CO_MUNICIPIO,
                rlEstabServClass.CO_SERVICO,
                rlEstabServClass.CO_CLASSIFICACAO,
                tbClassificacaoServico.DS_CLASSIFICACAO_SERVICO)\
            .withColumn('SK_REGISTRO', concat_ws('_',rlEstabServClass.CO_UNIDADE,rlEstabServClass.CO_SERVICO,rlEstabServClass.CO_CLASSIFICACAO))\
            .withColumn('DATA_INGESTAO', to_date(current_timestamp()))

    df_serv = df_serv_joined.drop_duplicates(['SK_REGISTRO'])

    df_joined = tbCargaHorariaSus\
    .join(tbAtividadeProfissional, tbCargaHorariaSus.CO_CBO ==tbAtividadeProfissional.CO_CBO)\
    .join(estab_munic, tbCargaHorariaSus.CO_UNIDADE == estab_munic.CO_UNIDADE)\
    .join(tbDadosProfissionalSus, tbCargaHorariaSus.CO_PROFISSIONAL_SUS == tbDadosProfissionalSus.CO_PROFISSIONAL_SUS)\
    .select(tbCargaHorariaSus.CO_UNIDADE,
            tbCargaHorariaSus.CO_PROFISSIONAL_SUS,
            tbDadosProfissionalSus.NO_PROFISSIONAL,
            tbCargaHorariaSus.CO_CBO,
            tbCargaHorariaSus.TP_SUS_NAO_SUS,            
            tbAtividadeProfissional.DS_ATIVIDADE_PROFISSIONAL,
            estab_munic.NO_FANTASIA,
            estab_munic.NO_BAIRRO,
            estab_munic.NO_MUNICIPIO,
            estab_munic.CO_MUNICIPIO,
            estab_munic.CO_SIGLA_ESTADO,
            estab_munic.CO_CEP,
            )\
    .withColumn('ds_localidade', concat_ws(',',estab_munic.CO_CEP,estab_munic.NO_MUNICIPIO,estab_munic.CO_SIGLA_ESTADO,lit("Brasil")))\
    .withColumn('SK_REGISTRO', concat_ws('_',tbCargaHorariaSus.CO_UNIDADE,tbCargaHorariaSus.CO_PROFISSIONAL_SUS,tbCargaHorariaSus.CO_CBO))\
    .withColumn('DATA_INGESTAO', to_date(current_timestamp()))
    
    df_final = df_joined.drop_duplicates(['SK_REGISTRO'])

    # Writing spark DataFrames to the Curated layer
    write_curated_file(df_final, 'curated_estabelecimentos', ano_mes)
    write_curated_file(df_serv, 'curated_servicos', ano_mes)

    # Updating PostgreSQL tables with files from the last layer
    update_table('curated_servicos', 'curated_servicos', ano_mes)
    update_table('curated_estabelecimentos', 'curated_estabelecimentos', ano_mes)
