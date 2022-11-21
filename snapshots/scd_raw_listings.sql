{% snapshot scd_raw_listings %}
    {{
        config(
            target_schema='raw',
            unique_key="id||'-'||name",
            strategy='timestamp',
            updated_at='updated_at',
            invalidate_hard_deletes=True
        )
    }}

    select * from {{ source('airbnb','listings') }}
 /*   select transaction_id || '-' || line_item_id as key ,* from {{ source('airbnb', 'listings') }} */

 {% endsnapshot %}