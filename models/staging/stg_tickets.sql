with source as (
    select * from {{ source('arteche_raw', 'tickets') }}
),

renamed as (
    select
        cast(request_id as integer) as ticket_id,
        trim(technician) as tecnico,
        trim(site) as sitio,
        trim(priority) as prioridad,
        
        -- CAMBIO AQUÍ: Usamos try_cast para ignorar textos basura
        try_cast(created_time as timestamp) as fecha_creacion,
        try_cast(resolved_time as timestamp) as fecha_resolucion,
        
        cast(fcr as boolean) as es_fcr,
        cast(re_opened as boolean) as fue_reabierto
    from source
)

select * from renamed
