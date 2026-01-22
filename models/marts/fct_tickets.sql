with tickets_limpios as (
    select * from {{ ref('stg_tickets') }}
),

calculos as (
    select
        *,
        -- Solo calcula si ambas fechas existen
        case 
            when fecha_resolucion is not null then date_diff('minute', fecha_creacion, fecha_resolucion) / 60.0 
            else null 
        end as horas_resolucion,
        
        -- Ajustamos el estatus para tickets abiertos
        case 
            when fecha_resolucion is null then 'Ticket Abierto'
            when (date_diff('minute', fecha_creacion, fecha_resolucion) / 60.0) <= 4 then 'Cumple SLA'
            else 'Fuera de SLA'
        end as estatus_sla
    from tickets_limpios
)

select * from calculos
