# Compare from Spreadsheet - Guía de Uso

## 🎯 **Funcionalidad Agregada**

He añadido dos nuevos comandos al CLI para comparar datasets desde Google Sheets:

### 1. 📊 **compare-spreadsheet** 
Compara múltiples datasets usando una configuración en Google Sheets

### 2. 📋 **compare-inventory**
Compara datasets desde el inventario existente en Google Sheets

## 🚀 **Cómo usar**

### **Comando 1: Compare from Spreadsheet**

```bash
# Uso básico (usa variables de entorno)
python src/argo_migration/cli.py compare-spreadsheet

# Con parámetros específicos
python src/argo_migration/cli.py compare-spreadsheet \
    --spreadsheet-id "1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA" \
    --sheet-name "QA - Test" \
    --credentials "/path/to/credentials.json" \
    --sampling-method random
```

### **Comando 2: Compare from Inventory**

```bash
# Uso básico (usa variables de entorno)
python src/argo_migration/cli.py compare-inventory

# Con credenciales específicas
python src/argo_migration/cli.py compare-inventory \
    --credentials "/path/to/credentials.json" \
    --sampling-method ordered
```

## 🔧 **Variables de Entorno Requeridas**

```bash
# Google Sheets
export GOOGLE_SHEETS_CREDENTIALS_FILE="/path/to/your/credentials.json"

# Para compare-spreadsheet
export COMPARISON_SPREADSHEET_ID="your_comparison_sheet_id"
export COMPARISON_SHEET_NAME="QA - Test"  # opcional, default: "QA - Test"

# Para compare-inventory  
export MIGRATION_SPREADSHEET_ID="your_main_spreadsheet_id"
export INTERMEDIATE_MODELS_SHEET_NAME="Inventory"  # opcional, default: "Inventory"

# Domo
export DOMO_DEVELOPER_TOKEN="your_domo_token"
export DOMO_INSTANCE="your_instance"

# Snowflake
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

## 📋 **Formato de Spreadsheet**

### **Para compare-spreadsheet ("QA - Test" sheet)**

Tu spreadsheet debe tener estas columnas:

| Output ID | Table Name | Key Columns | Transform Columns | Sample Size | Notes |
|-----------|------------|-------------|------------------|-------------|--------|
| abc123    | raw_sales  | id,date     | true            | 5000        |        |
| def456    | raw_users  | user_id     | false           | 1000        |        |

### **Para compare-inventory ("Inventory" sheet)**

Tu spreadsheet de inventario debe tener:

| Output ID | Model Name | Key Columns | ... |
|-----------|------------|-------------|-----|
| abc123    | sales_data | id,date     | ... |
| def456    | user_data  | user_id     | ... |

## 📊 **Lo que hace**

1. **🔗 Conecta** a Domo y Snowflake automáticamente
2. **📖 Lee** la configuración desde Google Sheets
3. **🔍 Compara** cada dataset con su tabla correspondiente en Snowflake
4. **📈 Genera** reportes detallados de las comparaciones
5. **✅ Actualiza** el spreadsheet con resultados (si tiene permisos de escritura)

## 🎉 **Ejemplo de Salida**

```
🚀 Starting spreadsheet-based comparisons...
🔧 Comparison Configuration:
   Spreadsheet ID: 1Y_CpIXW9RCxnlwwvP-tAL5B9UmvQlgu6DbpEnHgSgVA
   Sheet Name: QA - Test
   Credentials: /path/to/credentials.json
   Sampling Method: random

🔗 Setting up connections...
✅ Connections established
📊 Running comparisons from spreadsheet...

🔄 Comparing dataset abc123 vs table raw_sales
✅ Dataset abc123: Comparison completed - Perfect match!

🔄 Comparing dataset def456 vs table raw_users  
⚠️  Dataset def456: Comparison completed - Discrepancies found

📊 Comparison Summary:
   📋 Total comparisons: 2
   ✅ Successful: 2
   ❌ Failed: 0

🎉 All spreadsheet comparisons completed successfully!
```

## 🛠️ **Troubleshooting**

### **Error: No valid credentials**
```bash
# Asegúrate de que el archivo existe
ls -la /path/to/credentials.json

# Configura la variable de entorno
export GOOGLE_SHEETS_CREDENTIALS_FILE="/path/to/credentials.json"
```

### **Error: Spreadsheet ID not found**
```bash
# Configura los IDs de spreadsheet
export COMPARISON_SPREADSHEET_ID="your_sheet_id"
export MIGRATION_SPREADSHEET_ID="your_main_sheet_id"
```

### **Error: Connection failed**
```bash
# Prueba las conexiones individuales
python src/argo_migration/cli.py test-domo
python src/argo_migration/cli.py test-snowflake
```

## 🎯 **Próximos Pasos**

1. **Configura** tus variables de entorno
2. **Prepara** tu spreadsheet con el formato correcto
3. **Ejecuta** una prueba con pocos datasets
4. **Revisa** los resultados y ajusta según necesites

¡Listo para usar! 🚀
