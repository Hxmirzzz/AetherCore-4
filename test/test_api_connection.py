import logging
from src.infrastructure.di.container import ApplicationContainer
from src.application.dto.servicio_dto import AetherServiceImportDto

logging.basicConfig(level=logging.INFO)

def test_flow():
    container = ApplicationContainer()
    api = container.api_service()

    print("--- 1. Probando Autenticación ---")
    if api.login():
        print("✅ Login exitoso.")
    else:
        print("❌ Fallo en el login. Revisa el .env y el endpoint /Account/Login en C#.")
        return

    print("\n--- 2. Probando Envío de DTO Dummy ---")
    test_dto = AetherServiceImportDto(
        cod_cliente=4,
        cod_sucursal=1,
        fecha_solicitud="2026-03-01", 
        hora_solicitud="10:00:00",
        cod_concepto=1,
        cod_punto_origen="0001",
        cod_punto_destino="",
        observaciones="Prueba de conexión AC4"
    )

    result = api.upload_services([test_dto])
    if result:
        print(f"✅ Respuesta de la API: {result['summary']}")
    else:
        print("❌ Fallo en el envío.")

if __name__ == "__main__":
    test_flow()