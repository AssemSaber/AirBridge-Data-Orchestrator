{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%} -- u get the defualt target schema provided in profiles.yml 
    {%- if custom_schema_name is none -%}     -- if no customized the schema in {{ config()}} or project.yml, which is none, enter and use the defualt schema 

        {{ default_schema }}

    {%- else -%}  -- else >>> the customized the schema

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}