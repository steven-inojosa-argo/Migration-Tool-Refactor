import re

def create_stg_sql_file(columns: list[dict], source_schema_name: str, source_table_name: str, output_filename: str = "file.sql", use_cast: bool = False) -> str:
    """
    Crea un archivo SQL de staging a partir de una lista de columnas con sus tipos de datos.
    
    Args:
        columns: Lista de diccionarios con 'name' y 'data_type' keys
        source_schema_name: Nombre del schema fuente
        source_table_name: Nombre de la tabla fuente  
        output_filename: Nombre del archivo SQL a generar (por defecto "file.sql")
        use_cast: Si se debe usar CAST explícito en las columnas (por defecto False)
    
    Returns:
        str: El contenido SQL generado
    """
    
    def sanitize_column_name(col_name: str) -> str:
        """
        Convierte el nombre de columna a minúsculas, sustituye espacios por guiones bajos
        y elimina caracteres que no sean letras, números o guiones bajos.
        """
        alias = col_name.replace(" ", "_")
        alias = re.sub(r"[^0-9A-Za-z_]", "", alias)
        return alias.lower()

    def get_cast_expression(column_name: str, data_type: str) -> str:
        """
        Genera la expresión CAST apropiada con CAST explícito en TODAS las columnas.
        Mantiene la compatibilidad Domo → Snowflake pero con tipos consistentes.
        
        Args:
            column_name: Nombre de la columna
            data_type: Tipo de dato (puede ser de Domo o Snowflake)
            
        Returns:
            str: Expresión SQL con CAST explícito, keywords en minúsculas
        """
        # Convertir tipo de dato a minúsculas para consistencia
        data_type_lower = data_type.lower()
        
        # TODAS las columnas tendrán CAST explícito para:
        # 1. Documentación clara del tipo esperado
        # 2. Consistencia en el patrón SQL
        # 3. Validación explícita de tipos
        # 4. Mejor mantenibilidad
        return f'cast("{column_name}" as {data_type_lower})'

    def generate_sql(columns: list[dict], use_cast_param: bool = False) -> str:
        """
        Dada una lista de columnas con tipos, genera el bloque SQL con CAST explícito en TODAS las columnas:
        
        with
            source as (select * from {{ source("SRC", "VW_TABLE_NAME") }})
        select
            cast("column_original" as varchar(255)) as column_original,
            cast("created_at" as timestamp) as created_at,
            cast("amount" as number(10,2)) as amount,
            ...
        from source
        """
        # Convertir tabla a mayúsculas para el source
        table_name_upper = source_table_name.upper()
        schema_name_upper = source_schema_name.upper()
        
        header = (
            "with\n"
            f"    source as (select * from {{{{ source(\"{schema_name_upper}\", \"{table_name_upper}\") }}}})\n\n"
            "select\n"
        )
        
        # Separate regular columns from commented columns
        regular_columns = [col for col in columns if not col.get('commented', False)]
        commented_columns = [col for col in columns if col.get('commented', False)]
        
        # Generate regular column lines
        if use_cast_param:
            body_lines = [
                f'    {get_cast_expression(col["name"], col["data_type"])} as {sanitize_column_name(col["name"])}'
                for col in regular_columns
            ]
        else:
            body_lines = [
                f'    "{col["name"]}" as {sanitize_column_name(col["name"])}'
                for col in regular_columns
            ]
        body = ",\n".join(body_lines)
        
        # Add commented columns at the end if any
        footer = "\n\nfrom source"
        if commented_columns:
            commented_lines = []
            
            # Separate different types of commented columns
            domo_only = [col for col in commented_columns if col.get('comment_type') == 'domo_only']
            snowflake_only = [col for col in commented_columns if col.get('comment_type') == 'snowflake_only']
            other_commented = [col for col in commented_columns if not col.get('comment_type')]
            
            # Add Domo-only columns comments
            if domo_only:
                commented_lines.append("\n\n-- Columns found in Domo but not in Snowflake:")
                for col in domo_only:
                    if use_cast_param:
                        commented_line = f"    -- {get_cast_expression(col['name'], col['data_type'])} as {sanitize_column_name(col['name'])}  -- Domo: {col.get('domo_name', col['name'])} ({col.get('domo_type', 'UNKNOWN')})"
                    else:
                        commented_line = f"    -- \"{col['name']}\" as {sanitize_column_name(col['name'])}  -- Domo: {col.get('domo_name', col['name'])} ({col.get('domo_type', 'UNKNOWN')})"
                    commented_lines.append(commented_line)
            
            # Add Snowflake-only columns comments
            if snowflake_only:
                commented_lines.append("\n\n-- Columns found in Snowflake but not in Domo:")
                for col in snowflake_only:
                    if use_cast_param:
                        commented_line = f"    -- {get_cast_expression(col['name'], col['data_type'])} as {sanitize_column_name(col['name'])}  -- Snowflake: {col['name']} ({col['data_type']})"
                    else:
                        commented_line = f"    -- \"{col['name']}\" as {sanitize_column_name(col['name'])}  -- Snowflake: {col['name']} ({col['data_type']})"
                    commented_lines.append(commented_line)
            
            # Add other commented columns (legacy support)
            if other_commented:
                commented_lines.append("\n\n-- Other columns:")
                for col in other_commented:
                    if use_cast_param:
                        commented_line = f"    -- {get_cast_expression(col['name'], col['data_type'])} as {sanitize_column_name(col['name'])}  -- {col.get('domo_name', col['name'])} ({col.get('domo_type', 'UNKNOWN')})"
                    else:
                        commented_line = f"    -- \"{col['name']}\" as {sanitize_column_name(col['name'])}  -- {col.get('domo_name', col['name'])} ({col.get('domo_type', 'UNKNOWN')})"
                    commented_lines.append(commented_line)
            
            footer = "\n".join(commented_lines) + footer
        
        return header + body + footer

    # Generar el SQL
    sql_content = generate_sql(columns, use_cast)
    
    # Escribir el archivo
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(sql_content)
    
    print(f"Archivo '{output_filename}' generado exitosamente.")
    
    return sql_content


# Ejemplo de uso (solo para testing)
if __name__ == "__main__":
    # Lista de ejemplo para testing - estructura nueva con nombres y tipos
    example_columns = [
        {"name": "active", "data_type": "BOOLEAN"},
        {"name": "amazon_inbound_shipment_plan_id", "data_type": "VARCHAR(100)"},
        {"name": "amazon_reference_id", "data_type": "VARCHAR(50)"},
        {"name": "amazon_seller_id", "data_type": "VARCHAR(50)"},
        {"name": "are_cases_required", "data_type": "BOOLEAN"},
        {"name": "box_content_fee_per_unit", "data_type": "NUMBER(10,2)"},
        {"name": "box_content_total_fee", "data_type": "NUMBER(10,2)"},
        {"name": "box_content_total_units", "data_type": "INTEGER"},
        {"name": "box_contents_source", "data_type": "VARCHAR(100)"},
        {"name": "box_items_limit_allowed", "data_type": "INTEGER"},
        {"name": "carrier_description", "data_type": "VARCHAR(255)"},
        {"name": "carrier_id", "data_type": "VARCHAR(50)"},
        {"name": "closed_at", "data_type": "TIMESTAMP"},
        {"name": "confirmed_need_by_date", "data_type": "DATE"},
        {"name": "country_id", "data_type": "VARCHAR(10)"},
        {"name": "created_at", "data_type": "TIMESTAMP"},
        {"name": "deleted_at", "data_type": "TIMESTAMP"},
        {"name": "deleted_by", "data_type": "VARCHAR(100)"},
        {"name": "destination_fulfillment_center_id", "data_type": "VARCHAR(50)"},
        {"name": "expiration_dates_required", "data_type": "BOOLEAN"},
        {"name": "fc_prep_required", "data_type": "BOOLEAN"},
        {"name": "fnsku_or_upc", "data_type": "VARCHAR(50)"},
        {"name": "from_add_line1", "data_type": "VARCHAR(255)"},
        {"name": "from_add_line2", "data_type": "VARCHAR(255)"},
        {"name": "from_city", "data_type": "VARCHAR(100)"},
        {"name": "from_postal", "data_type": "VARCHAR(20)"},
        {"name": "from_state", "data_type": "VARCHAR(50)"},
        {"name": "gate_packing", "data_type": "VARCHAR(100)"},
        {"name": "id", "data_type": "INTEGER"},
        {"name": "is_ns_transfer_order", "data_type": "BOOLEAN"},
        {"name": "label_prep_type", "data_type": "VARCHAR(100)"},
        {"name": "labeling_required", "data_type": "BOOLEAN"},
        {"name": "max_number_items_per_box", "data_type": "INTEGER"},
        {"name": "ns_warehouse_name_id", "data_type": "VARCHAR(50)"},
        {"name": "number_of_pallets", "data_type": "INTEGER"},
        {"name": "overweight", "data_type": "BOOLEAN"},
        {"name": "overweight_max", "data_type": "NUMBER(10,2)"},
        {"name": "settlement_amount", "data_type": "NUMBER(15,2)"},
        {"name": "settlement_id", "data_type": "VARCHAR(100)"},
        {"name": "settlement_report_id", "data_type": "VARCHAR(100)"},
        {"name": "settlement_transaction_id", "data_type": "VARCHAR(100)"},
        {"name": "shipment_address_id", "data_type": "INTEGER"},
        {"name": "shipment_id", "data_type": "INTEGER"},
        {"name": "shipment_name", "data_type": "VARCHAR(255)"},
        {"name": "shipment_status", "data_type": "VARCHAR(50)"},
        {"name": "shipping_labels_1000", "data_type": "INTEGER"},
        {"name": "shipping_labels_required", "data_type": "BOOLEAN"},
        {"name": "tracking_no", "data_type": "VARCHAR(100)"},
        {"name": "transparent_by", "data_type": "VARCHAR(100)"},
        {"name": "transparent_on", "data_type": "TIMESTAMP"},
        {"name": "transport_estimated_cost", "data_type": "NUMBER(10,2)"},
        {"name": "transport_shipment_type", "data_type": "VARCHAR(50)"},
        {"name": "transport_status", "data_type": "VARCHAR(50)"},
        {"name": "updated_at", "data_type": "TIMESTAMP"},
        {"name": "updated_source", "data_type": "VARCHAR(100)"},
        {"name": "warehouse_id", "data_type": "INTEGER"},
        {"name": "weight_of_all_pallets", "data_type": "NUMBER(10,2)"},
        {"name": "whs_status_at", "data_type": "TIMESTAMP"},
        {"name": "whs_status_by", "data_type": "VARCHAR(100)"},
        {"name": "whs_status_id", "data_type": "INTEGER"},
        {"name": "whs_statuses_json", "data_type": "VARCHAR(16777216)"},
        {"name": "warehouse_shipped_date", "data_type": "DATE"},
        {"name": "_BATCH_ID_", "data_type": "VARCHAR(100)"},
        {"name": "_BATCH_LAST_RUN_", "data_type": "TIMESTAMP"}
    ]
    
    # Crear el archivo SQL usando las columnas de ejemplo
    create_stg_sql_file(
        columns=example_columns,
        source_schema_name="src",
        source_table_name="vw_amazon_shipments",
        output_filename="stg_amazon_shipments.sql",
        use_cast=False  # Ejemplo sin CAST por defecto
    )