import json
import logging
import timeit
from datetime import datetime
from io import BytesIO
from typing import Any
import pandas as pd
import dapla as dp
import pyarrow as pa
import fastavro
import duckdb

DEST_BUCKET_NAME = "gs://ssb-sirius-editering-data-produkt-prod/test"


# ##### Resultregnskap  ######

SKATT_NAERING_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(                  
                --- driftsinntekt

                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.driftsinntekt.salgsinntekt.inntekt)

                UNION ALL                
                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.driftsinntekt.annenDriftsinntekt.inntekt)

                -- driftskostnad
                                
                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.varekostnad.kostnad)

                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.loennskostnad.kostnad)

                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.annenDriftskostnad.kostnad)

                -- finansinntekt
                UNION ALL    
                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.finansinntekt.inntekt)

                -- finanskostnad
                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.finanskostnad.kostnad)
                          
                -- 9000 sumDriftsinntekt
                UNION ALL
                SELECT {'felt' : 'sumDriftsinntekt', 'beloep' : resultatregnskap.driftsinntekt.sumDriftsinntekt}

                -- 9010 sumDriftskostnad
                UNION ALL
                SELECT {'felt' : 'sumDriftskostnad', 'beloep' : resultatregnskap.driftskostnad.sumDriftskostnad}

                -- 9060 sumFinansinntekt
                UNION ALL
                SELECT {'felt' : 'sumFinansinntekt', 'beloep' : resultatregnskap.sumFinansinntekt}

                -- 9070 sumFinanskostnad
                UNION ALL
                SELECT {'felt' : 'sumFinanskostnad', 'beloep' : resultatregnskap.sumFinanskostnad}

                -- 9200 aarsresultat
                UNION ALL
                SELECT {'felt' : 'aarsresultat', 'beloep' : resultatregnskap.aarsresultat}

                -- 9300 sumBalanseverdiForAnleggsmiddel
                UNION ALL   
                SELECT {
                    'felt' : 'sumBalanseverdiForAnleggsmiddel', 
                    'beloep' : balanseregnskap.anleggsmiddel.sumBalanseverdiForAnleggsmiddel
                }

                -- 9350 sumBalanseverdiForOmloepsmiddel
                UNION ALL   
                SELECT {
                    'felt' : 'sumBalanseverdiForOmloepsmiddel', 
                    'beloep' : balanseregnskap.omloepsmiddel.sumBalanseverdiForOmloepsmiddel
                }

                -- 9400 sumBalanseverdiForEiendel
                UNION ALL   
                SELECT {
                    'felt' : 'sumBalanseverdiForEiendel', 
                    'beloep' : balanseregnskap.sumBalanseverdiForEiendel
                }

                -- 9450 sumEgenkapital
                UNION ALL   
                SELECT {
                    'felt' : 'sumEgenkapital', 
                    'beloep' : balanseregnskap.gjeldOgEgenkapital.sumEgenkapital
                }
                                                        
                -- 9500 sumLangsiktigGjeld
                UNION ALL   
                SELECT {
                    'felt' : 'sumLangsiktigGjeld', 
                    'beloep' : balanseregnskap.gjeldOgEgenkapital.sumLangsiktigGjeld
                }

                -- 9550 sumKortsiktigGjeld
                UNION ALL   
                SELECT {
                    'felt' : 'sumKortsiktigGjeld', 
                    'beloep' : balanseregnskap.gjeldOgEgenkapital.sumKortsiktigGjeld
                }

                -- 9650 sumGjeldOgEgenkapital
                UNION ALL   
                SELECT {
                    'felt' : 'sumGjeldOgEgenkapital', 
                    'beloep' : balanseregnskap.sumGjeldOgEgenkapital
                }
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        inntektsaar,
        norskIdentifikator AS id,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        registreringstidspunkt,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


SKOG_OG_TOEMMERKONTO_SQL = """
    WITH skog_og_toemmerkonto AS (
        SELECT
            naeringsspesifikasjon.norskIdentifikator AS norskIdentifikator,
            CAST(naeringsspesifikasjon.inntektsaar AS INT64) AS inntektsaar,
            hendelse.registreringstidspunkt AS registreringstidspunkt,
            hendelse.sekvensnummer AS sekvensnummer,
            UNNEST(naeringsspesifikasjon.skogbruk.skogOgToemmerkonto, recursive := true)
        FROM 
            arrow_table
        WHERE 
            ARRAY_LENGTH(naeringsspesifikasjon.skogbruk.skogOgToemmerkonto) > 0 
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1
    ), 
    skog_og_toemmerkonto_unpivot AS ( 
        UNPIVOT skog_og_toemmerkonto
        ON COLUMNS(* EXCLUDE (norskIdentifikator,inntektsaar,registreringstidspunkt,sekvensnummer,id,driftsenhet,skogfond))
        INTO
            NAME felt
            VALUE beloep
    )
    
    SELECT skog_og_toemmerkonto_unpivot.* EXCLUDE (skogfond),
    FROM skog_og_toemmerkonto_unpivot 
    WHERE beloep != 0.0
    ORDER BY norskIdentifikator, inntektsaar, id, felt
"""

SKOGFOND_SQL = """
    WITH skogfond AS (
        SELECT
            naeringsspesifikasjon.norskIdentifikator AS norskIdentifikator,
            CAST(naeringsspesifikasjon.inntektsaar AS INT64) AS inntektsaar,
            hendelse.registreringstidspunkt AS registreringstidspunkt,
            hendelse.sekvensnummer AS sekvensnummer,
            skogOgToemmerkonto.id AS skogOgToemmerkontoId,
            UNNEST(skogOgToemmerkonto.skogfond, recursive := true)
        FROM
            arrow_table AS root,
            UNNEST(root.naeringsspesifikasjon.skogbruk.skogOgToemmerkonto)
        WHERE
            ARRAY_LENGTH(root.naeringsspesifikasjon.skogbruk.skogOgToemmerkonto) > 0            
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    ),
    skogfond_unpivot AS ( 
        UNPIVOT skogfond
        ON COLUMNS(* EXCLUDE (norskIdentifikator,inntektsaar,registreringstidspunkt,sekvensnummer,skogOgToemmerkontoId,id,kommunenummer))
        INTO
            NAME felt
            VALUE beloep
    )
    
    SELECT skogfond_unpivot.*
    FROM skogfond_unpivot 
    WHERE beloep != 0.0
    ORDER BY norskIdentifikator, inntektsaar, skogOgToemmerkontoId, kommunenummer, felt                      
"""

# +
DRIFTSINNTEKT_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(                  
                --- driftsinntekt

                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.driftsinntekt.salgsinntekt.inntekt)

                UNION ALL                
                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.driftsinntekt.annenDriftsinntekt.inntekt)

            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""




# -

DRIFTSKOSTNAD_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(            
            
                -- varekostnad
                                 
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.varekostnad.kostnad)
                
                --- loennskostnad

                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.loennskostnad.kostnad)
                
                --- annenDriftskostnad

                UNION ALL    
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.driftskostnad.annenDriftskostnad.kostnad)


            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


FINANSINNTEKT_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(            
            
                -- finansinntekt
  
                SELECT {'felt' : inntekt.type, 'beloep' : inntekt.beloep}
                FROM UNNEST(resultatregnskap.finansinntekt.inntekt)


            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

FINANSKOSTNAD_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(            
            
                -- finanskostnad
                SELECT {'felt' : kostnad.type, 'beloep' : kostnad.beloep}
                FROM UNNEST(resultatregnskap.finanskostnad.kostnad)


            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


SUM_RESULTATREGNSKAP_SQL = """
   WITH naeringsspesifikasjon_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(            
            
                -- 9000 sumDriftsinntekt
                SELECT {'felt' : 'sumDriftsinntekt', 'beloep' : resultatregnskap.driftsinntekt.sumDriftsinntekt}

                -- 9010 sumDriftskostnad
                UNION ALL
                SELECT {'felt' : 'sumDriftskostnad', 'beloep' : resultatregnskap.driftskostnad.sumDriftskostnad}

                -- 9060 sumFinansinntekt
                UNION ALL
                SELECT {'felt' : 'sumFinansinntekt', 'beloep' : resultatregnskap.sumFinansinntekt}

                -- 9070 sumFinanskostnad
                UNION ALL
                SELECT {'felt' : 'sumFinanskostnad', 'beloep' : resultatregnskap.sumFinanskostnad}

                -- 9200 aarsresultat
                UNION ALL
                SELECT {'felt' : 'aarsresultat', 'beloep' : resultatregnskap.aarsresultat}

            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        naeringsspesifikasjon_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

# ###### BALANSEREGNSKAP #######

BALANSEREGNSKAP_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                -- sumBalanseverdiForAnleggsmiddel
                SELECT {felt: 'sumBalanseverdiForAnleggsmiddel', beloep: balanseregnskap.anleggsmiddel.sumBalanseverdiForAnleggsmiddel}

                UNION ALL
                -- balanseverdiForAnleggsmiddel
                SELECT {felt: balanseverdi.type, beloep: balanseverdi.beloep}
                FROM UNNEST(balanseregnskap.anleggsmiddel.balanseverdiForAnleggsmiddel.balanseverdi)

                UNION ALL
                -- sumBalanseverdiForOmloepsmiddel
                SELECT {felt: 'sumBalanseverdiForOmloepsmiddel', beloep: balanseregnskap.omloepsmiddel.sumBalanseverdiForOmloepsmiddel}

                UNION ALL
                -- balanseverdiForOmloepsmiddel
                SELECT {felt: balanseverdi.type, beloep: balanseverdi.beloep}
                FROM UNNEST(balanseregnskap.omloepsmiddel.balanseverdiForOmloepsmiddel.balanseverdi) 

                UNION ALL
                -- sumLangsiktigGjeld
                SELECT {felt: 'sumLangsiktigGjeld', beloep: balanseregnskap.gjeldOgEgenkapital.sumLangsiktigGjeld}

                UNION ALL
                -- sumKortsiktigGjeld
                SELECT {felt: 'sumKortsiktigGjeld', beloep: balanseregnskap.gjeldOgEgenkapital.sumKortsiktigGjeld}

                UNION ALL
                -- sumEgenkapital
                SELECT {felt: 'sumEgenkapital', beloep: balanseregnskap.gjeldOgEgenkapital.sumEgenkapital}

                UNION ALL
                -- langsiktigGjeld
                SELECT {felt: gjeld.type, beloep: gjeld.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.langsiktigGjeld.gjeld) 

                UNION ALL
                -- kortsiktigGjeld
                SELECT {felt: gjeld.type, beloep: gjeld.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.kortsiktigGjeld.gjeld) 

                UNION ALL
                -- egenkapital
                SELECT {felt: kapital.type, beloep: kapital.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.egenkapital.kapital) 

                UNION ALL
                -- sumBalanseverdiForEiendel
                SELECT {felt: 'sumBalanseverdiForEiendel', beloep: balanseregnskap.sumBalanseverdiForEiendel}

                UNION ALL
                -- sumGjeldOgEgenkapital
                SELECT {felt: 'sumGjeldOgEgenkapital', beloep: balanseregnskap.sumGjeldOgEgenkapital}

            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        inntektsaar,
        norskIdentifikator AS id,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        registreringstidspunkt,
        sekvensnummer
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


SUM_BALANSERVERDI_FOR_ANLEGGSMIDDEL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
            
                -- sumBalanseverdiForAnleggsmiddel
                SELECT {'felt' : 'sumBalanseverdiForAnleggsmiddel', 'beloep' : balanseregnskap.anleggsmiddel.sumBalanseverdiForAnleggsmiddel}
                
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep,
        sekvensnummer
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


BALANSERVERDI_FOR_ANLEGGSMIDDEL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : balanseverdi.type, 'beloep' : balanseverdi.beloep}
                FROM UNNEST(balanseregnskap.anleggsmiddel.balanseverdiForAnleggsmiddel.balanseverdi)
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_BALANSERVERDI_FOR_OMLOEPSMIDDEL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumBalanseverdiForOmloepsmiddel', 'beloep' : balanseregnskap.omloepsmiddel.sumBalanseverdiForOmloepsmiddel}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

BALANSERVERDI_FOR_OMLOEPSMIDDEL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : balanseverdi.type, 'beloep' : balanseverdi.beloep}
                FROM UNNEST(balanseregnskap.omloepsmiddel.balanseverdiForOmloepsmiddel.balanseverdi) 
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_LANGSIKTIGGJELD_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumLangsiktigGjeld', 'beloep' : balanseregnskap.gjeldOgEgenkapital.sumLangsiktigGjeld}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_KORTSIKTIGGJELD_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumKortsiktigGjeld', 'beloep' : balanseregnskap.gjeldOgEgenkapital.sumKortsiktigGjeld}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_EGENKAPITAL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumEgenkapital', 'beloep' : balanseregnskap.gjeldOgEgenkapital.sumEgenkapital}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

LANGSIKTIGGJELD_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : gjeld.type, 'beloep' : gjeld.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.langsiktigGjeld.gjeld) 
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

KORTSIKTIGGJELD_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : gjeld.type, 'beloep' : gjeld.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.kortsiktigGjeld.gjeld)
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

EGENKAPITAL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : kapital.type, 'beloep' : kapital.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.egenkapital.kapital) 
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


SUM_GJELD_INNEN_BANK_OG_FORSIKRING_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumGjeldInnenBankOgForsikring', 'beloep' : balanseregnskap.gjeldOgEgenkapital.sumGjeldInnenBankOgForsikring}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

GJELD_INNEN_BANK_OG_FORSIKRING_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : gjeld.type, 'beloep' : gjeld.beloep}
                FROM UNNEST(balanseregnskap.gjeldOgEgenkapital.gjeldInnenBankOgForsikring.gjeld) 
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_BALANSERVERDI_FOR_EIENDEL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumBalanseverdiForEiendel', 'beloep' : balanseregnskap.sumBalanseverdiForEiendel}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""

SUM_GJELD_OG_EGENKAPITAL_SQL = """
   WITH balanseregnskap_numbers AS (
        SELECT
            norskIdentifikator,
            CAST(inntektsaar AS INT64) AS inntektsaar,
            registreringstidspunkt,
            sekvensnummer,
            ARRAY(
                SELECT {'felt' : 'sumGjeldOgEgenkapital', 'beloep' : balanseregnskap.sumGjeldOgEgenkapital}
            ) AS type_and_amount
        FROM
            (SELECT 
                naeringsspesifikasjon.*,
                hendelse.registreringstidspunkt,
                hendelse.sekvensnummer
            FROM 
                arrow_table 
            WHERE
                naeringsspesifikasjon.norskIdentifikator IS NOT NULL
                AND naeringsspesifikasjon.inntektsaar IS NOT NULL
            ) AS naeringsspesifikasjon
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY norskIdentifikator, inntektsaar ORDER BY registreringstidspunkt DESC) = 1            
    )

    SELECT
        sekvensnummer,
        type_and_amount_unnested.type_and_amount.felt,
        type_and_amount_unnested.type_and_amount.beloep
    FROM
        balanseregnskap_numbers AS root,
        UNNEST(root.type_and_amount) AS type_and_amount_unnested
    WHERE 
        type_and_amount_unnested.type_and_amount.beloep IS NOT NULL        
        AND type_and_amount_unnested.type_and_amount.beloep != 0.0
"""


def convert_timestamp_string_to_iso_format(timestamp_as_string: str) -> datetime:
    if '.' in timestamp_as_string:
        date_part, fractional_part = timestamp_as_string.split('.')
        fractional_part = fractional_part.rstrip('Z')  # Remove 'Z' if it's there
        fractional_part = (fractional_part + '000000')[:6]  # Pad to microseconds
        timestamp_as_string = f"{date_part}.{fractional_part}Z"
    index_of_z = min(timestamp_as_string.upper().find('Z'), 23)
    return datetime.fromisoformat(
        f"{timestamp_as_string[:index_of_z]}+00:00"
    )


def read_avro_into_records(avro_content: BytesIO) -> list[dict[str, Any]]:
    """Reads an Avro file into list of dicts.

    @param avro_content: The Avro file content.
    @return: The Avro file content as a list of dicts.
    """
    avro_reader = fastavro.reader(avro_content)
    _records: list[dict[str, Any]] = []

    # read first n records
    for avro_record in avro_reader:  # type: dict[str, Any]
        # for production, we would use the following line
        hendelse: dict[str, Any] = json.loads(avro_record["data"]["hendelse"])
        naeringsspesifikasjon: dict[str, Any] = json.loads(avro_record["data"]["naeringsspesifikasjon"])

        # replace registreringstidspunkt with the datetime (UTC)
        hendelse["registreringstidspunkt"] = convert_timestamp_string_to_iso_format(
            hendelse['registreringstidspunkt']
        )

        _records.append({
            "hendelse": hendelse,
            "naeringsspesifikasjon": naeringsspesifikasjon
        })

    return _records


def create_filename(file_prefix: str) -> str:
    """Creates a filename.

    @param file_prefix: The prefix to use for the filename.
    @return: The filename.
    """
    return f"{DEST_BUCKET_NAME}/{file_prefix}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"


def process_section(records: list[dict[str, Any]],
                    duckdb_sql: str,
                    file_prefix: str) -> str:
    """Processes the section data.

    @param records: The data to process.
    @param duckdb_sql: The SQL to use for processing the data.
    @param file_prefix: The prefix to use for the filename.
    @return: The destination filename.
    """
    arrow_table = pa.Table.from_pylist(records)
    logging.info("Number of records in arrow_table: %d", arrow_table.num_rows)
    result_df = duckdb.sql(duckdb_sql).to_df()

    destination_filename = create_filename(file_prefix)
    dp.write_pandas(
        df=result_df,
        gcs_path=destination_filename,
        file_format="parquet"
    )
    logging.info("Wrote Parquet file with %d records to %s",
                 result_df.shape[0], destination_filename)
    return destination_filename

def main(source_file: str) -> None:
    """
    Function for processing kildedata to inndata.
    Function takes kilde-bucket path as input and processes data, writing inndata
    to bucket.
    
    Parameters
    ----------
    source_file: str
        Google Cloud Storage filepath in kilde-bucket.
    """
    start_time = timeit.default_timer()

    with (dp.FileClient.get_gcs_file_system().open(path=source_file, mode="rb") as avro_file):
        naering_records = read_avro_into_records(avro_content=BytesIO(avro_file.read()))

        logging.info("Completed reading %s into records in %.3g seconds",
                     source_file,
                     timeit.default_timer() - start_time)

        if len(naering_records) < 1:
            logging.warning("No records found for original")
            return None

        # write records with original structure to Parquet
        dp.write_pandas(
            df=pd.DataFrame.from_records(naering_records),
            gcs_path=create_filename("opprinnelig-struktur"),
            file_format="parquet"
        )

        #
        # process the sections
        #

        process_section(
            records=naering_records,
            duckdb_sql=SKOG_OG_TOEMMERKONTO_SQL,
            file_prefix="skogbruk-skog-og-toemmerkonto"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SKOGFOND_SQL,
            file_prefix="skogbruk-skogfond"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SKATT_NAERING_SQL,
            file_prefix="naering"
        )
        
        process_section(
            records=naering_records,
            duckdb_sql=DRIFTSINNTEKT_SQL,
            file_prefix="driftsinntekt"
        )
        
        process_section(
            records=naering_records,
            duckdb_sql=DRIFTSKOSTNAD_SQL,
            file_prefix="driftskostnad"
        )
        
        
        process_section(
            records=naering_records,
            duckdb_sql=FINANSINNTEKT_SQL,
            file_prefix="finansinntekt"
        )
        
        process_section(
            records=naering_records,
            duckdb_sql=FINANSKOSTNAD_SQL,
            file_prefix="finanskostnad"
        )
        
        process_section(
            records=naering_records,
            duckdb_sql=SUM_RESULTATREGNSKAP_SQL,
            file_prefix="sum_resultatregnskap"
        )
        

        process_section(
            records=naering_records,
            duckdb_sql=BALANSEREGNSKAP_SQL,
            file_prefix="balanseregnskap"
        )
        
        process_section(
            records=naering_records,
            duckdb_sql=SUM_BALANSERVERDI_FOR_ANLEGGSMIDDEL_SQL,
            file_prefix="sum-balanseregnskap-anleggsmiddel"
        )

        process_section(
            records=naering_records,
            duckdb_sql=BALANSERVERDI_FOR_ANLEGGSMIDDEL_SQL,
            file_prefix="balanseverdi-anleggsmiddel"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SUM_BALANSERVERDI_FOR_OMLOEPSMIDDEL_SQL,
            file_prefix="sum-balanseregnskap-omloepsmiddel"
        )

        process_section(
            records=naering_records,
            duckdb_sql=BALANSERVERDI_FOR_OMLOEPSMIDDEL_SQL,
            file_prefix="balanseverdi-omloepsmiddel"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SUM_LANGSIKTIGGJELD_SQL,
            file_prefix="sum-langsiktig-gjeld"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SUM_KORTSIKTIGGJELD_SQL,
            file_prefix="sum-kortsiktig-gjeld"
        )

        process_section(
            records=naering_records,
            duckdb_sql=SUM_EGENKAPITAL_SQL,
            file_prefix="sum-egenkapital"
        )

        process_section(
            records=naering_records,
            duckdb_sql=LANGSIKTIGGJELD_SQL,
            file_prefix="langsiktig-gjeld"
        )

        process_section(
            records=naering_records,
            duckdb_sql=KORTSIKTIGGJELD_SQL,
            file_prefix="kortsiktig-gjeld"
        )

        process_section(
            records=naering_records,
            duckdb_sql=EGENKAPITAL_SQL,
            file_prefix="egenkapital"
        )

    logging.info("Completed processing %s in %.3g seconds",
                 source_file,
                 timeit.default_timer() - start_time)

main("gs://ssb-prod-skatt-naering-data-kilde/naeringsspesifikasjon_data/g2023/naeringsspesifikasjon_p2023_v1.avro/2024-04-18T10:35:24.628Z_9244_67451671_864877.avro")
