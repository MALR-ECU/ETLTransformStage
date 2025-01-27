import azure.functions as func
import logging
from services.sql_operations_staging import Crear_Tabla_Staging, get_max_created_at, insert_into_staging
from services.sql_operations_registroinspeccion import manage_registers_DB

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="transformstageDM")
def transformstageDM(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        
        try:
            Crear_Tabla_Staging()
        except Exception as e:
            logging.error(f"Error inesperado al crear/verificar la tabla: {e}")
            return func.HttpResponse(f"Error inesperado al crear/verificar la tabla: {e}", status_code=500)

        try:
            FechaCargaBlob = get_max_created_at()
        except Exception as e:
            logging.error(f"Error al obtener la fecha máxima: {e}")
            return func.HttpResponse("Error interno al procesar fecha máxima registroinspeccion.", status_code=500)

        try:
            df_consulta_registro_inspeccion = manage_registers_DB(FechaCargaBlob)
        except Exception as e:
            logging.error(f"Error Consulta resgistros de inspección nuevos: {e}")
            return func.HttpResponse(f"Error Consulta resgistros de inspección nuevos: {e}", status_code=500)
        
        try:
            if df_consulta_registro_inspeccion.empty:
                logging.info("El DataFrame está vacío. No hay registros para insertar en la tabla de staging.")
                return func.HttpResponse("El DataFrame está vacío. No se realizó ninguna acción.", status_code=200)
            else:
                insert_into_staging(df_consulta_registro_inspeccion)
        except Exception as e:
            logging.error(f"Error inesperado ingreso a staging: {e}")
            return func.HttpResponse(f"Error inesperado ingreso a staging: {e}", status_code=500)
        
        return func.HttpResponse("Consolidación completada, transformación exitosa.", status_code=200)

    except Exception as e:
        logging.error(f"Error inesperado en el proceso: {e}")
        return func.HttpResponse(f"Error inesperado en el proceso: {e}", status_code=500)    