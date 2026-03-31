# Fintech Data Pipeline - Technical Challenge

Este proyecto consiste en el diseño e implementación de un sistema de procesamiento de datos end-to-end para una fintech, enfocado en la ingesta, limpieza y detección de anomalías en transacciones financieras.

## Arquitectura de la Fase 1: Data Lake e Ingestión

En esta etapa inicial se ha establecido la infraestructura base, priorizando la escalabilidad y la persistencia de datos mediante un modelo de almacenamiento de objetos.

### Componentes Técnicos
* **Data Lake (MinIO):** Se ha desplegado una instancia de MinIO mediante Docker Compose, proporcionando una capa de almacenamiento persistente compatible con el protocolo S3. Esto asegura la interoperabilidad con servicios de nube como AWS.
* **Patrón Staging & Cleanup:** El pipeline utiliza el sistema de archivos local únicamente como un área de tránsito temporal (*staging*). Una vez confirmada la carga exitosa en el bucket de MinIO, el recurso local es liberado para optimizar el almacenamiento del nodo de cómputo.
* **Procesamiento en Memoria:** Tras la sincronización con el Data Lake, los datos se transfieren directamente como DataFrames de Pandas a las funciones de procesamiento, reduciendo latencias de I/O innecesarias.

---

## Configuración del Entorno

### Requisitos
* Python 3.11+
* Docker y Docker Compose
* Virtualenv

### Instrucciones de Despliegue

1. **Clonar el repositorio:**
   ```bash
   git clone git@github.com:J-Lopez-IICG/Technical-Challenge-JavierLopez
   cd Technical-Challenge-JavierLopez

### Preparar el entorno virtual
* python -m venv venv
* source venv/bin/activate  # En Windows: .\venv\Scripts\activate
* pip install -r requirements.txt

### Iniciar infraestructura
docker-compose up -d
* Consola de Administración http://localhost:9001
* Credenciales por defecto: admin/password123

## Estado de Avance: Fase 1

| Objetivo | Estado | Descripción Técnica | Tiempo Estimado |
| :--- | :--- | :--- | :--- |
| **Ingesta de Datos** | Completado | Generación y lectura de archivos CSV en intervalos de 60s. | 45 min |
| **Integración MinIO** | Completado | Implementación de cliente Boto3 para persistencia en S3. | 1h 15 min |
| **Gestión de Archivos** | Completado | Implementación de limpieza automática de staging local. | 30 min |
| **Infraestructura Docker** | Completado | Orquestación de servicios mediante Docker Compose. | 30 min |

> **Nota sobre el Cronograma:** El tiempo total invertido en la Fase 1 fue de **3 horas**. Este tiempo incluye la configuración del entorno de contenedores, la validación de la conectividad con la API de S3 y la reestructuración del orquestador principal para soportar el procesamiento en memoria.

## Decisiones de Ingeniería y Arquitectura

* **Persistencia Híbrida:** Se implementó un esquema donde los datos aterrizan primero en un `staging` local para asegurar la integridad antes de ser transferidos al Data Lake (MinIO).
* **Eficiencia de Recursos:** El sistema elimina automáticamente los archivos temporales tras una subida exitosa, cumpliendo con las mejores prácticas de gestión de almacenamiento en nodos de cómputo.
* **Agnosticismo de Nube:** Al utilizar el SDK `boto3`, el código es compatible con AWS S3, permitiendo una migración a producción con cambios mínimos en la configuración.

### Flujo de Datos de la Fase 1

```mermaid
graph LR
    %% Nodos
    Generador[Script Python generate_transactions.py]

    subgraph Local_Node [Local Compute Node]
        Main[Orchestrator main.py]
        Staging[/"Local CSV Area transactions"/]
    end

    subgraph Docker_Stack [MinIO Docker Stack]
        MinIO[(Persistent Storage MinIO)]
    end

    %% Conexiones
    Generador ==> Main
    Main ==>|1. Save local| Staging
    Main -.->|2. S3 Sync boto3| MinIO
    Staging -.->|3. Cleanup| Main

    %% Estilos de Alto Contraste
    classDef blue fill:#004080,stroke:#333,stroke-width:2px,color:#fff;
    classDef orange fill:#d98c00,stroke:#333,stroke-width:2px,color:#fff;
    classDef lightblue fill:#add8e6,stroke:#333,stroke-width:2px,color:#000;
    classDef green fill:#28a745,stroke:#333,stroke-width:2px,color:#fff;

    class Generador blue;
    class Main orange;
    class Staging lightblue;
    class MinIO green;