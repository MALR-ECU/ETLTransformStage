import logging
import pymssql
import os

sql_server = os.environ["SQL_SERVER"]  # Dirección del servidor SQL
sql_username = os.environ["SQL_USERNAME"]  # Usuario de SQL Server
sql_password = os.environ["SQL_PASSWORD"]  # Contraseña de SQL Server
sql_table_string = "LandingDB.Staging.StagingInspecciones"
sql_database, schema_name, table_name = sql_table_string.split('.') 

def Crear_Tabla_Staging():
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Verificar si la tabla existe
            create_table_query = f"""
            IF NOT EXISTS (
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            )
            BEGIN
                CREATE TABLE {schema_name}.{table_name} (
                    [ID] INT IDENTITY(1,1) PRIMARY KEY,
                    [ID_Inspeccion] BIGINT,
                    [Operador] VARCHAR(MAX) NULL,
                    [Equipo] INT NULL,
                    [Turno] INT NULL,
                    [Conexion] VARCHAR(MAX) NULL,
                    [Diametro] VARCHAR(MAX) NULL,
                    [Orden_de_Produccion] INT NULL,
                    [Lado] VARCHAR(50) NULL,
                    [Colada] INT NULL,
                    [Codigo_Unico] NVARCHAR(MAX) NULL,
                    [Variacion_de_Diametro] FLOAT NULL,
                    [Ovalidad] FLOAT NULL,
                    [Paso] FLOAT NULL,
                    [Conicidad] FLOAT NULL,
                    [Longitud_de_Rosca] FLOAT NULL,
                    [Altura_de_Rosca] FLOAT NULL,
                    [Perfil_de_Rosca] VARCHAR(50) NULL,
                    [Espesor_de_Cara] VARCHAR(50) NULL,
                    [Estado] VARCHAR(50) NULL,
                    [Motivo_Descarte] VARCHAR(50) NULL,
                    [Comentario] VARCHAR(MAX) NULL,
                    [Month] INT NULL,
                    [Day] INT NULL,
                    [Year] INT NULL,
                    [Hour] TIME NULL,
                    [FechaCargaBlob] DATETIME
                )
            END
            """
            cursor.execute(create_table_query)
            conn.commit()
            logging.info(f"Tabla '{schema_name}.{table_name}' verificada/creada exitosamente.")
    
    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al crear/verificar la tabla: {e}")
        raise

def get_max_created_at():
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Consulta para obtener la fecha máxima
            query_max_fecha = f"SELECT MAX(FechaCargaBlob) AS MaxFecha FROM {schema_name}.{table_name}"
            cursor.execute(query_max_fecha)
            max_fecha = cursor.fetchone()[0]  # Obtener la fecha mayor como string
            
            # Si no hay registros previos, usar una fecha inicial por defecto
            if max_fecha is None:
                max_fecha = '2000-01-01'  # Fecha base inicial
            
            logging.info(f"Fecha máxima en FechaCargaBlob encontrada: {max_fecha}")
            return max_fecha
        
    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        raise

def insert_into_staging(data):
    try:
        # Conexión a SQL Server
        with pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database
        ) as conn:
            cursor = conn.cursor()

            # Recorrer cada fila del DataFrame
            for _, row in data.iterrows():
                query_merge = f"""
                MERGE INTO {schema_name}.{table_name} AS Target
                USING (SELECT 
                        '{row['ID_Inspeccion']}' AS ID_Inspeccion,
                        '{row['Operador']}' AS Operador,
                        '{row['Equipo']}' AS Equipo,
                        '{row['Turno']}' AS Turno,
                        '{row['Conexion']}' AS Conexion,
                        '{row['Diametro']}' AS Diametro,
                        '{row['Orden de Produccion']}' AS Orden_de_Produccion,
                        '{row['Lado']}' AS Lado,
                        '{row['Colada']}' AS Colada,
                        '{row['Codigo Unico']}' AS Codigo_Unico,
                        '{row['Varicion de Diametro']}' AS Variacion_de_Diametro,
                        '{row['Ovalidad']}' AS Ovalidad,
                        '{row['Paso']}' AS Paso,
                        '{row['Conicidad']}' AS Conicidad,
                        '{row['Longitud de rosca']}' AS Longitud_de_Rosca,
                        '{row['Altura de rosca']}' AS Altura_de_Rosca,
                        '{row['Perfil de Rosca']}' AS Perfil_de_Rosca,
                        '{row['Espesor de cara']}' AS Espesor_de_Cara,
                        '{row['Estado']}' AS Estado,
                        '{row['Motivo Descarte']}' AS Motivo_Descarte,
                        '{row['Comentario']}' AS Comentario,
                        '{row['Month']}' AS Month,
                        '{row['Day']}' AS Day,
                        '{row['Year']}' AS Year,
                        '{row['Hour']}' AS Hour,
                        '{row['CreatedAt'].strftime('%Y-%m-%d %H:%M:%S')}' AS CreatedAt
                      ) AS Source
                ON Target.ID_Inspeccion = Source.ID_Inspeccion
                WHEN MATCHED THEN
                    UPDATE SET 
                        Operador = Source.Operador,
                        Equipo = Source.Equipo,
                        Turno = Source.Turno,
                        Conexion = Source.Conexion,
                        Diametro = Source.Diametro,
                        Orden_de_Produccion = Source.Orden_de_Produccion,
                        Lado = Source.Lado,
                        Colada = Source.Colada,
                        Codigo_Unico = Source.Codigo_Unico,
                        Variacion_de_Diametro = Source.Variacion_de_Diametro,
                        Ovalidad = Source.Ovalidad,
                        Paso = Source.Paso,
                        Conicidad = Source.Conicidad,
                        Longitud_de_Rosca = Source.Longitud_de_Rosca,
                        Altura_de_Rosca = Source.Altura_de_Rosca,
                        Perfil_de_Rosca = Source.Perfil_de_Rosca,
                        Espesor_de_Cara = Source.Espesor_de_Cara,
                        Estado = Source.Estado,
                        Motivo_Descarte = Source.Motivo_Descarte,
                        Comentario = Source.Comentario,
                        Month = Source.Month,
                        Day = Source.Day,
                        Year = Source.Year,
                        Hour = Source.Hour,
                        FechaCargaBlob = Source.CreatedAt
                WHEN NOT MATCHED THEN
                    INSERT (
                        ID_Inspeccion, Operador, Equipo, Turno, Conexion, Diametro,
                        Orden_de_Produccion, Lado, Colada, Codigo_Unico, Variacion_de_Diametro,
                        Ovalidad, Paso, Conicidad, Longitud_de_Rosca, Altura_de_Rosca,
                        Perfil_de_Rosca, Espesor_de_Cara, Estado, Motivo_Descarte, Comentario,
                        Month, Day, Year, Hour, FechaCargaBlob
                    )
                    VALUES (
                        Source.ID_Inspeccion, Source.Operador, Source.Equipo, Source.Turno, 
                        Source.Conexion, Source.Diametro, Source.Orden_de_Produccion, Source.Lado, 
                        Source.Colada, Source.Codigo_Unico, Source.Variacion_de_Diametro, Source.Ovalidad, 
                        Source.Paso, Source.Conicidad, Source.Longitud_de_Rosca, Source.Altura_de_Rosca, 
                        Source.Perfil_de_Rosca, Source.Espesor_de_Cara, Source.Estado, Source.Motivo_Descarte, 
                        Source.Comentario, Source.Month, Source.Day, Source.Year, Source.Hour, Source.CreatedAt
                    );
                """
                cursor.execute(query_merge)

            conn.commit()
            logging.info(f"Operación MERGE completada. {len(data)} registros procesados.")

    except pymssql.OperationalError as e:
        logging.error(f"Error de conexión a SQL Server: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        raise