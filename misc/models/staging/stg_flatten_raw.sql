WITH base AS (
    SELECT
        _AIRBYTE_RAW_ID AS batch_id,  
        _AIRBYTE_EXTRACTED_AT AS load_timestamp,  
        features
    FROM
        {{ source('earthquake', 'earthquake_data_raw') }}
),

-- Flatten the array of JSON objects
flattened_features AS (
    SELECT
        base.batch_id,                                  
        base.load_timestamp,                          
        COALESCE(f.value:geometry:coordinates[0]::string, 'NULL') AS longitude, 
        COALESCE(f.value:geometry:coordinates[1]::string, 'NULL') AS latitude,   
        COALESCE(f.value:geometry:coordinates[2]::string, 'NULL') AS depth,      
        COALESCE(f.value:id::string, 'NULL') AS id,                             
        COALESCE(f.value:properties:mag::string, 'NULL') AS magnitude,           
        COALESCE(f.value:properties:place::string, 'NULL') AS location,       
        COALESCE(TO_TIMESTAMP_NTZ(f.value:properties:time::number / 1000)::string, 'NULL') AS event_time,  
        COALESCE(TO_TIMESTAMP_NTZ(f.value:properties:updated::number / 1000)::string, 'NULL') AS updated_time,  
        COALESCE(f.value:properties:url::string, 'NULL') AS event_url,         
        COALESCE(f.value:properties:felt::string, 'NULL') AS felt_reports,     
        COALESCE(f.value:properties:cdi::string, 'NULL') AS cdi,                
        COALESCE(f.value:properties:mmi::string, 'NULL') AS mmi,                 
        COALESCE(f.value:properties:alert::string, 'NULL') AS alert_level,      
        COALESCE(f.value:properties:status::string, 'NULL') AS status,        
        COALESCE(f.value:properties:tsunami::string, 'NULL') AS tsunami,      
        COALESCE(f.value:properties:sig::string, 'NULL') AS significance,      
        COALESCE(f.value:properties:net::string, 'NULL') AS network,           
        COALESCE(f.value:properties:code::string, 'NULL') AS code,              
        COALESCE(f.value:properties:nst::string, 'NULL') AS num_stations,       
        COALESCE(f.value:properties:dmin::string, 'NULL') AS min_distance,      
        COALESCE(f.value:properties:rms::string, 'NULL') AS rms,                
        COALESCE(f.value:properties:gap::string, 'NULL') AS gap,                
        COALESCE(f.value:properties:magType::string, 'NULL') AS mag_type,       
        COALESCE(f.value:properties:type::string, 'NULL') AS event_type,        
        COALESCE(f.value:properties:title::string, 'NULL') AS event_title,      
        MD5(
            COALESCE(longitude, 'NULL') || '|' ||
            COALESCE(latitude, 'NULL') || '|' ||
            COALESCE(depth, 'NULL') || '|' ||
            COALESCE(id, 'NULL') || '|' ||
            COALESCE(magnitude, 'NULL') || '|' ||
            COALESCE(location, 'NULL') || '|' ||
            COALESCE(event_time, 'NULL') || '|' ||
            COALESCE(event_url, 'NULL') || '|' ||
            COALESCE(felt_reports, 'NULL') || '|' ||
            COALESCE(cdi, 'NULL') || '|' ||
            COALESCE(mmi, 'NULL') || '|' ||
            COALESCE(alert_level, 'NULL') || '|' ||
            COALESCE(status, 'NULL') || '|' ||
            COALESCE(tsunami, 'NULL') || '|' ||
            COALESCE(significance, 'NULL') || '|' ||
            COALESCE(network, 'NULL') || '|' ||
            COALESCE(code, 'NULL') || '|' ||
            COALESCE(num_stations, 'NULL') || '|' ||
            COALESCE(min_distance, 'NULL') || '|' ||
            COALESCE(rms, 'NULL') || '|' ||
            COALESCE(gap, 'NULL') || '|' ||
            COALESCE(mag_type, 'NULL') || '|' ||
            COALESCE(event_type, 'NULL') || '|' ||
            COALESCE(event_title, 'NULL')
        ) AS load_id
    FROM
        base,
        LATERAL FLATTEN(input => base.features) f
)

SELECT
    *
FROM flattened_features
