# AetherCore 4 - Procesamiento de Archivos

## Descripción General

Aplicación Python para el procesamiento automatizado de archivos Excel con arquitectura limpia (Clean Architecture) y patrón de inyección de dependencias. Transforma archivos de entrada en formatos estructurados, genera respuestas de estado e integra los datos procesados con 3 APIs externas para la gestión de servicios, órdenes y notificaciones.

## Características

* **Arquitectura Limpia**
  * Separación clara de responsabilidades en capas (dominio, aplicación, infraestructura, presentación)
  * Bajo acoplamiento y alta cohesión
  * Fácil de mantener y extender

* **Procesamiento de Archivos**
  * Soporte para archivos Excel (.xlsx, .xlsm)
  * Monitoreo en tiempo real de carpetas de entrada
  * Procesamiento por cliente y solicitud con estructura organizada
  * Generación de reportes en Excel con formato profesional
  * Generación de archivos de respuesta con estado de procesamiento

* **Integración con APIs**
  * **VCash API (Interna)**: Carga masiva de servicios, consulta de clientes autorizados, registro y actualización de eventos
  * **External API**: Creación de órdenes de servicio (individual y bulk), obtención de mapeo de clientes y tipos de servicio
  * **Lumen API**: Sistema de notificaciones y alertas para errores críticos y eventos importantes
  * Autenticación mediante tokens Bearer/JWT con reintentos automáticos
  * Manejo robusto de errores de conexión y timeouts
  * Logging detallado de payloads y respuestas API

* **Mapeo de Datos**
  * Mapeo de datos de Excel a DTOs estructurados
  * Conversión de formatos para integración con APIs
  * Validación de datos antes del envío a APIs
  * Manejo de múltiples clientes y solicitudes simultáneas

* **Manejo de Errores**
  * Sistema de logging centralizado con rotación automática
  * Manejo robusto de excepciones en todos los niveles
  * Registro detallado de errores con trazabilidad completa
  * Archivos fallidos se mueven a carpetas de errores con respuestas de rechazo

* **Seguridad**
  * Gestión segura de credenciales mediante variables de entorno
  * Validación de datos de entrada en múltiples capas
  * Control de acceso a archivos y recursos

* **Instalación como Servicio de Windows**
  * Script automatizado para instalación mediante NSSM (Non-Sucking Service Manager)
  * Configuración de inicio automático con el sistema
  * Monitoreo continuo de carpetas de entrada
  * Logs dedicados del servicio con rotación automática
  * Reinicio automático en caso de fallos

## Estructura del Proyecto

```
AetherCore/
├── src/
│   ├── application/               # Capa de aplicación
│   │   ├── dto/                   # Objetos de transferencia de datos
│   │   ├── interfaces/            # Interfaces para casos de uso
│   │   ├── orchestrators/         # Orquestadores de flujos de trabajo
│   │   ├── processors/            # Procesadores específicos (TXT/XML)
│   │   └── services/              # Servocios de aplicacion
│   │
│   ├── domain/                    # Capa de dominio
│   │   ├── entities/              # Entidades del dominio
│   │   ├── exceptions/            # Excepciones del dominio
│   │   ├── repositories/          # Interfaces de repositorios
│   │   └── value_objects/         # Objetos de valor
│   │
│   ├── infrastructure/            # Capa de infraestructura
│   │   ├── config/                # Configuración
│   │   ├── di/                    # Inyección de dependencias
│   │   ├── excel/                 # Manejo de archivos Excel
│   │   ├── file_system/           # Operaciones de sistema de archivos
│   │   ├── logging/               # Ajustes para logs
│   │   └── notifications/         # Clientes de notificación (Lumen API)
│   │
│   └── presentation/              # Capa de presentación
│       ├── api/                   # Clientes de APIs (VCash, External)
│       └── console/               # Interfaz de línea de comandos
│
├── config/                        # Archivos de configuración YAML
├── logs/                          # Logs del sistema
├── tests/                         # Pruebas automatizadas
├── .env.example                   # Plantilla de variables de entorno
├── .env                           # Variables de entorno (no versionado)
├── requirements.txt               # Dependencias del proyecto
├── install_windows_service.bat    # Instalador del servicio de Windows
└── README.md                      # Documentación
```

## Requisitos del Sistema

* **Sistema Operativo:** Windows, Linux, o macOS
* **Python:** 3.8 o superior
* **Dependencias:**
  * pandas
  * openpyxl
  * requests
  * python-dotenv
  * pydantic>=2
  * pyyaml>=6
  * pydantic-settings
* **Conectividad:**
  * Acceso a las 3 APIs (VCash, External, Lumen)
  * Conexión a internet para llamadas API

## Configuración

### 1. Variables de Entorno

Cree un archivo `.env` en la raíz del proyecto basado en `.env.example`:

```ini
# =========================================================
# AMBIENTE Y CONFIGURACIÓN GENERAL
# =========================================================
APP_ENV=DEV
REFERENCE_SOURCE=ENUM
TIEMPO_ESPERA_MONITOREO_GENERAL=10
SYNC_APIS=false

# =========================================================
# RUTA DE CARPETAS EXCLUSIVAS
# =========================================================
BASE_DIR=C:\AetherCore

# =========================================================
# CONFIGURACIÓN DE APIS
# =========================================================
# VCash API (Interna) - Para carga de servicios y eventos
VCASH_API_URL=your_api_url
AC4_AUTH_USER=your_username
AC4_AUTH_PASSWORD=your_secure_password

# External API - Para órdenes de servicio
EXTERNAL_API_URL=your_api_url
EXTERNAL_API_USER=your_username
EXTERNAL_API_PASSWORD=your_secure_password
API_BULK_LIMIT=10

# Lumen API - Para notificaciones y alertas
LUMEN_API_URL=lumen_api_url
LUMEN_API_KEY=lumen_api_key

# Configuración de Logging
LOG_LEVEL=INFO
LOG_FILE=./logs/aethercore.log
```

### 2. Estructura de Carpetas

La aplicación espera la siguiente estructura de carpetas:

```
C:\AetherCore\
├── [Cliente]/
│   ├── [Solicitud]/
│   │   ├── archivo.xlsx    # Archivos Excel de entrada
│   │   └── errores/        # Archivos con errores
│   └── ...
└── logs/                  # Archivos de registro
```

### 3. Configuración de APIs

#### a) VCash API (Interna)

Esta API se utiliza para:
- Carga masiva de servicios procesados
- Consulta de clientes autorizados
- Registro de eventos de procesamiento
- Actualización de estado de eventos

**Endpoints principales:**
- `POST /Auth/Login` - Autenticación
- `POST /AetherCore/upload-services` - Carga masiva de servicios
- `GET /AetherCore/clients` - Obtener lista de clientes
- `POST /AetherCore/log` - Registrar evento
- `PUT /AetherCore/log/{id}` - Actualizar evento

#### b) External API

Esta API se utiliza para:
- Creación de órdenes de servicio individuales
- Creación masiva de órdenes (bulk)
- Obtener mapeo de clientes
- Obtener tipos de servicio

**Endpoints principales:**
- `POST /auth/login/` - Autenticación
- `POST /service-orders/` - Crear orden individual
- `POST /service-orders/bulk/` - Crear órdenes masivas
- `GET /clients/` - Obtener mapeo de clientes
- `GET /service-types/` - Obtener tipos de servicio

#### c) Lumen API

Esta API se utiliza para:
- Envío de notificaciones de errores críticos
- Alertas de eventos importantes
- Monitoreo de estado del sistema

**Configuración:**
- URL del endpoint de notificaciones
- API Key para autenticación
- Canales de destino (email, SMS, etc.)

## Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/Hxmirzzz/AetherCore.git
cd AetherCore
```

### 2. Configurar Entorno Virtual

```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno (Windows)
.\venv\Scripts\activate

# Activar entorno (Linux/macOS)
source venv/bin/activate
```

### 3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 4. Configuración Inicial

1. Copiar el archivo de ejemplo de configuración:
   ```bash
   copy .env.example .env
   ```

2. Editar el archivo `.env` con las credenciales y URLs de las 3 APIs.

3. Verificar conectividad a las APIs:
   ```bash
   python test/test_api_connection.py
   ```

### 5. Instalación como Servicio de Windows (Opcional)

Para ejecutar AetherCore como un servicio de Windows que inicia automáticamente con el sistema:

#### Requisitos Previos

1. **NSSM (Non-Sucking Service Manager):**
   - Descarga: https://nssm.cc/download
   - Extrae `nssm.exe` a `C:\Windows\System32` o agrégalo al PATH del sistema

2. **Privilegios de Administrador:**
   - El script `install_windows_service.bat` debe ejecutarse como administrador

#### Instalación Automática

1. **Ejecutar como Administrador:**
  ```
    Click derecho en install_windows_service.bat → "Ejecutar como administrador"
  ```

2. **El script realizará automáticamente:**
   - ✅ Verificación de privilegios de administrador
   - ✅ Detección automática de Python (probando `python`, `py` y ubicaciones comunes)
   - ✅ Creación del entorno virtual si no existe
   - ✅ Instalación de dependencias desde `requirements.txt`
   - ✅ Creación de carpetas necesarias (`data/in`, `data/out`, `logs`, etc.)
   - ✅ Copia de `.env.example` a `.env` (si no existe)
   - ✅ Instalación del servicio con NSSM
   - ✅ Configuración de logs con rotación automática (10MB máximo)
   - ✅ Configuración de reinicio automático en caso de fallos

3. **Configuración Post-Instalación:**
   
  Si es la primera instalación, edita el archivo `.env` con tus credenciales y rutas:
```
   notepad .env
```
   
  Luego inicia el servicio:
```
  nssm start AetherCoreService
```

#### Gestión del Servicio

Comandos útiles
```
# Iniciar servicio
nssm start AetherCoreService

# Detener servicio
nssm stop AetherCoreService

# Reiniciar servicio
nssm restart AetherCoreService

# Ver estado del servicio
nssm status AetherCoreService

# Editar configuración del servicio
nssm edit AetherCoreService

# Eliminar servicio
nssm stop AetherCoreService
nssm remove AetherCoreService confirm
```

**Gestión desde Windows:**
```
services.msc → Buscar "AetherCore File Processor"
```

#### Logs del Servicio

El servicio genera logs en múltiples ubicaciones:
```
logs/
├── service_stdout.log      # Salida estándar del servicio
├── service_stderr.log      # Errores del servicio
└── aethercore_*.log        # Logs de la aplicación
```

**Ver logs en tiempo real:**
```
# Logs del servicio
type logs\service_stdout.log

# Logs de la aplicación
type logs\aethercore_*.log
```

## Uso

### Modo Consola (Ejecución Manual)

La aplicación se ejecuta a través de la línea de comandos:

```bash
# Iniciar el monitoreo de carpetas en tiempo real
python run.py
```

La aplicación monitoreará automáticamente la carpeta configurada en `BASE_DIR` detectando archivos Excel (.xlsx, .xlsm) y procesándolos según la estructura de carpetas por cliente y solicitud.

## Monitoreo y Registros

### Estructura de Logs

La aplicación genera registros en la carpeta `logs/` con el siguiente formato de nombre:
```
aethercore_YYYY-MM-DD.log
```

### Niveles de Log

- **DEBUG**: Información detallada para depuración
- **INFO**: Eventos normales de la aplicación
- **WARNING**: Situaciones inusuales que no impiden la ejecución
- **ERROR**: Errores que afectan la funcionalidad
- **CRITICAL**: Errores graves que detienen la aplicación

### Configuración de Logs

Puede ajustar el nivel de log en el archivo `.env`:
```
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILE=./logs/aethercore.log
```

## Mantenimiento

### Actualización de Configuración de APIs

Las credenciales y configuraciones de APIs se cargan desde el archivo `.env` al iniciar la aplicación. Para actualizar:

1. Detener la aplicación
2. Editar el archivo `.env` con las nuevas credenciales
3. Reiniciar la aplicación

### Limpieza de Archivos Procesados

Se recomienda configurar una tarea programada para limpiar o archivar archivos antiguos en las carpetas de salida y procesados.

## Solución de Problemas

### Problemas Comunes

1. **Error de conexión a APIs**
   - Verificar credenciales en `.env`
   - Comprobar que las URLs sean correctas y accesibles
   - Verificar conectividad a internet
   - Revisar logs para detalles de errores HTTP

2. **Error de autenticación (401)**
   - Verificar que las credenciales sean correctas
   - Comprobar que el token no haya expirado
   - El sistema reintenta automáticamente la autenticación

3. **Archivos no se procesan**
   - Verificar permisos de las carpetas
   - Comprobar que los archivos tengan la extensión correcta (.xlsx, .xlsm)
   - Revisar los logs en busca de errores
   - Verificar la estructura de carpetas (Cliente/Solicitud)

4. **Timeout en llamadas API**
   - Verificar la latencia de la red
   - Aumentar los timeouts en la configuración si es necesario
   - Reducir el tamaño de los lotes bulk (API_BULK_LIMIT)

## Contribución

1. Hacer fork del repositorio
2. Crear una rama para la nueva característica (`git checkout -b feature/nueva-funcionalidad`)
3. Hacer commit de los cambios (`git commit -am 'Añadir nueva funcionalidad'`)
4. Hacer push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear un Pull Request

## Arquitectura de Integración API

### Flujo de Procesamiento

1. **Detección de Archivos**: El sistema monitorea la carpeta `BASE_DIR` detectando archivos Excel
2. **Procesamiento**: Los archivos se procesan y mapean a DTOs estructurados
3. **Integración VCash API**: Los servicios se cargan masivamente mediante la API interna
4. **Integración External API**: Las órdenes de servicio se crean en el sistema externo
5. **Notificaciones**: Eventos importantes se registran y errores críticos se notifican vía Lumen API

### Manejo de Errores

- **Reintentos automáticos**: Para errores de autenticación (401) y timeouts
- **Logging detallado**: Payloads y respuestas se registran para debugging
- **Notificaciones**: Errores críticos se envían a Lumen API
- **Archivos con errores**: Se mueven a carpetas de errores para revisión manual

## Contacto / Soporte
Para obtener ayuda o reportar problemas, por favor contacte con [Hxmirzzz](jamir08david@gmail.com)