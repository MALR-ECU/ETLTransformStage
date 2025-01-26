import logging
import pymssql
import pandas as pd
import os

# Variables de entorno
sql_server = os.environ["SQL_SERVER"]  
sql_username = os.environ["SQL_USERNAME"]  
sql_password = os.environ["SQL_PASSWORD"]  
sql_table_string = "LandingDB.InspectionsReports.RegistroInspecciones" 
sql_database, schema_name, table_name = sql_table_string.split('.')

def manage_registers_DB(FechaCargaBlob):
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            query_nuevos_registros = f"""
            WITH CTE AS (
                SELECT 
                    [ID],
                    LEFT(CONCAT(
                        CAST([Year] AS CHAR(4)), 
                        RIGHT('00' + CAST([Month] AS VARCHAR), 2), 
                        RIGHT('00' + CAST([Day] AS VARCHAR), 2), 
                        REPLACE([Hour], ':', '')
                    ), 14) AS ID_Inspeccion,
                    ROW_NUMBER() OVER (PARTITION BY 
                        CONCAT(CAST([Year] AS CHAR(4)), 
                            RIGHT('00' + CAST([Month] AS VARCHAR), 2), 
                            RIGHT('00' + CAST([Day] AS VARCHAR), 2), 
                            REPLACE([Hour], ':', '')) 
                        ORDER BY [CreatedAt] DESC) AS rn,
                    [Operador],
                    [Equipo],
                    [Turno],
                    [Conexion],
                    [Diametro],
                    [Orden de Produccion],
                    [Lado],
                    [Colada],
                    [Codigo Unico],
                    ISNULL([Varicion de Diametro], 0) AS [Varicion de Diametro],  
                    ISNULL([Ovalidad], 0) AS [Ovalidad],
                    ISNULL([Paso], 0) AS [Paso],
                    ISNULL([Conicidad], 0) AS [Conicidad],
                    ISNULL([Longitud de rosca], 0) AS [Longitud de rosca],
                    ISNULL([Altura de rosca], 0) AS [Altura de rosca],
                    [Perfil de Rosca],
                    [Espesor de cara],
                    [Estado],
                    [Motivo Descarte],
                    [Comentario],
                    [Month],
                    [Day],
                    [Year],
                    [Hour],
                    [CreatedAt]
                FROM {schema_name}.{table_name}
                WHERE CreatedAt > %s
                AND [Codigo Unico] IS NOT NULL
                AND [Estado] IS NOT NULL
                AND [Conexion] IS NOT NULL
                AND [Diametro] IS NOT NULL
                AND [Lado] IS NOT NULL
            )
            SELECT 
                ID_Inspeccion,
                [Operador],
                [Equipo],
                [Turno],
                [Conexion],
                [Diametro],
                [Orden de Produccion],
                [Lado],
                [Colada],
                [Codigo Unico],
                [Varicion de Diametro],  
                [Ovalidad],
                [Paso],
                [Conicidad],
                [Longitud de rosca],
                [Altura de rosca],
                [Perfil de Rosca],
                [Espesor de cara],
                [Estado],
                [Motivo Descarte],
                [Comentario],
                [Month],
                [Day],
                [Year],
                [Hour],
                [CreatedAt]
            FROM CTE
            WHERE rn = 1;
            """
            # Ejecutar la consulta
            cursor.execute(query_nuevos_registros, (FechaCargaBlob,))
            rows = cursor.fetchall()

            # Convertir a DataFrame
            columns = [column[0] for column in cursor.description]
            registro_df = pd.DataFrame.from_records(rows, columns=columns)
 
            logging.info(f"Consulta SQL: {query_nuevos_registros}")
            return registro_df
        
    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        raise