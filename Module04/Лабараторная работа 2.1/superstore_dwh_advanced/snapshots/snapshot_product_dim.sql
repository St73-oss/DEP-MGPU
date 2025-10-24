-- snapshots/snapshot_product_dim.sql
{% snapshot snapshot_product_dim %}
{{
    config(
      target_schema='dw_snapshots',
      strategy='check',
      unique_key='prod_id',
      check_cols=['segment', 'category'],
    )
}}
SELECT prod_id, product_id, segment, category FROM {{ source('dw_test', 'product_dim') }}
{% endsnapshot %}
