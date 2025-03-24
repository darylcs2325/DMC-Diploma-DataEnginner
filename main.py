import os
import pandas as pd
import logging
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def obtener_credenciales():
    """Obtener credenciales desde el archivo .env"""

    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")

    if not all([user, password, server, database]):
        logging.error("❌ Faltan credenciales en el archivo .env")
        raise ValueError("Las credenciales de la base de datos no están configuradas correctamente")
    
    logging.info("✅ Credenciales obtenidas correctamente")
    return user, password, server, database


def extract_data(file_path: str):
    """Extraer los datos del archivo .ope"""
    if not os.path.exists(file_path):
        logging.error("❌ El archivo no existe")
        raise FileNotFoundError("El archivo no existe")
    
    try:
        data = pd.read_csv(file_path)
        df_clientes = data[data['Field_1'].str[0]=="1"].reset_index(drop=True).copy()
        df_deuda = data[data['Field_1'].str[0]=="2"].reset_index(drop=True).copy()
        logging.info("✅ Datos extraídos correctamente")
    except Exception as e:
        logging.error(f"❌ Error al leer el archivo: {e}")

    return df_clientes, df_deuda

def transform_data(clientes: pd.DataFrame, deuda: pd.DataFrame):
    """Transformar los datos extraídos"""	

    try:
        df_clientes = clientes['Field_1'].str.split('|', expand=True).copy()
        columns_name = ['SBSCodigoCliente', 'SBSFechaReporte', 'SBSTipoDocumentoT',
            'SBSRucCliente', 'SBSTipoDocumento', 'SBSNumeroDocumento',
            'SBSTipoPer', 'SBSTipoEmpresa', 'SBSNumeroEntidad', 'SBSSalNor',
            'SBSSalCPP', 'SBSSalDEF', 'SBSSalDUD', 'SBSSalAPER', 'SBSAPEPAT',
            'SBSAPEMAT', 'SBSAPECAS', 'SBSNOMCLI', 'SBSNOMCLI2']
    
        df_clientes.columns = columns_name

        df_deuda = deuda.copy()
        df_deuda["CodigoSBS"]=df_deuda['Field_1'].str[:10]
        df_deuda["CodigoEmpresa"]=df_deuda['Field_1'].str[10:15]
        df_deuda["TipoCredito"]=df_deuda['Field_1'].str[15:17]
        df_deuda["Nivel2"]=df_deuda['Field_1'].str[17:19]
        df_deuda["Moneda"]=df_deuda['Field_1'].str[19]
        df_deuda["SubCodigoCuenta"]=df_deuda['Field_1'].str[20:31]
        df_deuda["Condicion"]=df_deuda['Field_1'].str[31:37]
        df_deuda["ValorSaldo"]=df_deuda['Field_1'].str[37:41]
        df_deuda["ClasificacionDeuda"]=df_deuda['Field_1'].str[41]
        df_deuda["CodigoCuenta"]=df_deuda["Nivel2"]+df_deuda["Moneda"]+df_deuda["SubCodigoCuenta"]

        df_deuda.drop(columns=['Field_1'], inplace=True)

        nombre_nuevos = {
            'CodigoSBS': 'Cod_SBS',
            'CodigoEmpresa': 'Cod_Emp',
            'TipoCredito': 'Tipo_Credit',
            'ValorSaldo': 'Val_Saldo',
            'ClasificacionDeuda': 'Clasif_Deu',
            'CodigoCuenta': 'Cod_Cuenta'
        }

        df_deuda.rename(columns=nombre_nuevos, inplace=True)
        logging.info("✅ Datos transformados correctamente")

        return df_clientes, df_deuda
    
    except Exception as e:
        logging.error(f"❌ Error al transformar los datos: {e}")
        raise

def load_data(carpeta_final: str, df_clientes: pd.DataFrame, df_deudas: pd.DataFrame):
    """Cargar los datos transformados a CSV y SQL Server"""

    df_clientes.to_csv(carpeta_final + 'clientes.csv', index=False)
    df_deudas.to_csv(carpeta_final + 'deudas.csv', index=False)

    # Guardar deudas en la base de datos
    user, password, server, database = obtener_credenciales()
    driver= "ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

    try:
        engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")
        logging.info("✅ Conexión a la base de datos exitosa")
        df_deudas.to_sql('tabla_deudas', con=engine, index=False, if_exists='replace')
        logging.info("✅ Datos cargados correctamente")
    except exc.SQLAlchemyError as e:
        logging.error(f"❌ Error al cargar los datos a SQL Server: {e}")
        raise


def main():
    """Función principal del ETL"""
    archivo_inicio = './server_inputs/file.ope'
    carpeta_final = './server_outputs/'

    try:
        logging.info("Iniciando el proceso ETL")
        clientes, deudas = extract_data(file_path=archivo_inicio)
        df_clientes, df_deudas = transform_data(clientes, deudas)
        load_data(carpeta_final, df_clientes, df_deudas)
        logging.info("Proceso ETL finalizado con éxito")
    except Exception as e:
        logging.error(f"❌ Error en el proceso ETL: {e}")

if __name__ == '__main__':
    main()