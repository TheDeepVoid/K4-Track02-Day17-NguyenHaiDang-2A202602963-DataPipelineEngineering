-- Silver entity table: MERGE on the key, newer LSN wins, deletes become tombstones.
-- The dbt twin of pipeline/silver.py::upsert_silver_tickets.
{{ config(
    unique_key='ticket_id',
    incremental_strategy='merge',
    merge_update_condition='DBT_INTERNAL_SOURCE._lsn > DBT_INTERNAL_DEST._lsn',
    on_schema_change='fail'
) }}

with changes as (
    select * from {{ ref('stg_ticket_changes') }}
    {% if is_incremental() %}
    -- high-water mark: only changes we have not applied yet
    where _lsn > (select coalesce(max(_lsn), 0) from {{ this }})
    {% endif %}
)
select
    ticket_id,
    user_id,
    {{ mask_pii('subject') }}  as subject,
    {{ mask_pii('body') }}     as body,
    case when (_op = 'd') then null else priority end as priority,
    case when (_op = 'd') then null else status end as status,
    case when (_op = 'd') then null else category end as category,
    case when (_op = 'd') then null else created_at end as created_at,
    case when (_op = 'd') then null else updated_at end as updated_at,
    (_op = 'd')                as is_deleted,
    _lsn,
    _batch_id
from changes
qualify row_number() over (partition by ticket_id order by _lsn desc, _batch_id desc) = 1
