# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#   kernelspec:
#     display_name: skatt-naering
#     language: python
#     name: skatt-naering
# ---

# # Avledninger for skjema RF-1175 (NO1 & NO5)
# ## Oppdatert for inntektsåret 2023 med 10 nye poster til basen

# I denne notebooken avledes de følgende variablene fra skatteobjektene for skjema RF-1175 (NO1 & NO5):
#
# | Skatteobjekt | Variabler |
# | :----------- | :-------- |
# | [1. virksomhet](#virksomhet) | O_15805; O_15806 |
# | [2. spesifikasjonAvVarelager](#spesifikasjonAvVarelager) | O_15764; O_15765; O_15766; O_15767; O_9669; O_17165; O_31625; O_15768 |
# | [3. spesifikasjonAvSkattemessigVerdiPaaFordring](#spesifikasjonAvSkattemessigVerdiPaaFordring) | NY_I_TEMA; O_6941; O_6940; O_6939; O_6944; O_6943; O_117; O_27430 |
# | [4. salgsinntekt](#salgsinntekt) | O_7360; O_7362; O_7364; O_15843 |
# | [5. annenDriftsinntekt](#annenDriftsinntekt) | O_7368; O_39729; O_7370; O_17163; O_32864; O_7374; O_NY_POST1; O_13676; O_7266; O_7376; O_37279 |
# | [6. varekostnad](#varekostnad) | O_7378; O_7380; O_7382; O_7384 |
# | [7. loennskostnad](#loennskostnad) | O_15844; O_15845; O_15846; O_7392; O_7396; O_27426 |
# | [8. annenDriftskostnad](#annenDriftskostnad) | O_15847; O_7400; O_15848; O_7404; O_7408; O_7406; O_15849; O_28121; O_15850; O_7269; O_15836; O_7414; O_7416; O_32836; O_7418; O_7422; O_15851; O_7273; O_15801; O_7424; O_7426; O_7428; O_7275; O_7277; O_7279; O_11334; O_7430; O_7432; O_15837; O_1202; O_34056; O_34058; O_7283; O_37281 |
# | [9. finansinntekt](#finansinntekt) | O_15852; O_15853 |
# | [10. finanskostnad](#finanskostnad) | O_15854; O_7441 |
# | [11. balanseverdiForAnleggsmiddel](#balanseverdiForAnleggsmiddel) | O_7445; O_2400; O_7447; O_15796; O_15795; O_36968; O_32838; O_7451; O_7454; O_7456; O_15792; O_15793; O_15794; O_15791; O_37283; O_15790; O_26535; O_7306; O_15838; O_37285 |
# | [12. balanseverdiForOmloepsmiddel](#balanseverdiForOmloepsmiddel) | O_15768; NY_ORID; O_18116; O_26537; O_7465; O_7467; O_7469; O_7471; O_15788; O_7473; O_7474; O_15812; O_7477; O_7315 |
# | [13. Egenkapital](#Egenkapital) | NY_ORID_AK_EK; O_7479; O_7481; O_7483; O_1471; O_15839; O_15803; O_37287 |
# | [14. LangsiktigGjeld](#LangsiktigGjeld) | O_7485; O_7327; O_7487;  |
# | [15. KortsiktigGjeld](#KortsiktigGjeld) | O_7489; O_7491; O_7493; O_7495; O_7497; O_7499; O_29065; O_7501; O_7503; O_7505; O_7507; O_NY_ORID_1; O_NY_ORID_2; O_7509 |
# | [16. sum_resultatregnskap](#sum_resultatregnskap) | O_7489; O_7491; O_7493; O_7495; O_7497; O_7499; O_7501; O_7503; O_7505; O_7507; O_7509 |
# | [17. sum_balanseregnskap](#sum_balanseregnskap) | O_NY_ORID_sum_bal1; O_NY_ORID_sum_bal2; O_15797; O_7318; O_7318 |
# | [22. fordelt beregnet naeringsinntekt](#fordelt_beregnet_naeringsinntekt) | O_19797; O_19798; O_19799 |
# ***

# ## Importerer moduler

# +
# %run ~/skatt-naering/src/settings.py
# %run ~/skatt-naering/production/naeringsspesifikasjon/config_naeringsspesifikasjon.py

from pyspark.sql import SparkSession

from nst import functions
from nst.utflating.balanseregnskap import *
from nst.utflating.fordeltBeregnetNaeringsinntekt import *
from nst.utflating.resultatregnskap import *
from nst.utflating.spesifikasjonAvOmloepsmiddel import *
from nst.utflating.spesifikasjonAvResultatregnskapOgBalanse import *
from nst.utflating.virksomhet import *


# -

# ## Initialiserer pyspark

functions.use_virtualenv_in_pyspark()
spark = SparkSession.builder.getOrCreate()

# ## Leser tverrsnitt

spark.read.parquet(TVERRSNITT_NAERINGSSPESIFIKASJON).createOrReplaceTempView(
    "naeringsspesifikasjon"
)

# +
# spark.table("naeringsspesifikasjon").printSchema()
# -

# <a id='virksomhet'></a>
# ### 1. virksomhet

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "virksomhet"

# Leser inn skatteobjektet
df = spark.sql(virksomhet).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_15805
df["O_15805"] = df["start"]

# Avleder O_15806
df["O_15806"] = df["slutt"]

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='spesifikasjonAvVarelager'></a>
# ### 2. spesifikasjonAvVarelager

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "spesifikasjonAvVarelager"

# Leser inn skatteobjektet
df = spark.sql(spesifikasjonAvVarelager).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_15764
df["O_15764"] = df["raavareOgInnkjoeptHalvfabrikata"]

# Avleder O_15765
df["O_15765"] = df["vareUnderTilvirkning"]

# Avleder O_15766
df["O_15766"] = df["ferdigTilvirketVare"]

# Avleder O_15767
df["O_15767"] = df["innkjoeptVareForVideresalg"]

# Avleder O_9669
df["O_9669"] = df["buskap"]

# Avleder O_17165
df["O_17165"] = df["selvprodusertVareBenyttetIEgenProduksjon"]

# Avleder O_31625
df["O_31625"] = df["reinPelsdyrOgPelsdyrskinnPaaLager"]

# Avleder O_15768
# Se også del 12. balanseverdiForOmloepsmiddel
df["O_15768"] = df["sumVerdiAvVarelager"]

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

df.head(100)

# <a id='spesifikasjonAvSkattemessigVerdiPaaFordring'></a>
# ### 3. spesifikasjonAvSkattemessigVerdiPaaFordring

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "spesifikasjonAvSkattemessigVerdiPaaFordring"

# Leser inn skatteobjektet
df = spark.sql(spesifikasjonAvSkattemessigVerdiPaaFordring).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]
# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder NY_I_TEMA (DENNE POSTEN SKAL IKKE KOBLES, REGNES OM Å VÆRE ALLEREDE MED I O_117)
# df['NY_I_TEMA1'] = df['skattemessigNedskrivningPaaKundefordringForNyetablertVirksomhet']

# Avleder O_6941:skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_6941"] = df["kundefordringOgIkkeFakturertDriftsinntekt"]

# Avleder O_6940: skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_6940"] = df["konstatertTapPaaKundefordring"]

# Avleder O_6939:skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_6939"] = df["konstatertTapPaaKundefordringIFjor"]

# Avleder O_6944:skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_6944"] = df["kredittsalg"]

# Avleder O_6943:skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_6943"] = df["kredittsalgIFjor"]

# Avleder O_117: - skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_117"] = df["skattemessigNedskrivningPaaKundefordring"]

# Avleder O_27430: skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_27430"] = df["skattemessigVerdiPaaKundefordring"]

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
# df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='salgsinntekt'></a>
# ### 4. salgsinntekt

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "salgsinntekt"

# Leser inn skatteobjektet
df = spark.sql(salgsinntekt).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]
# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7360
df["O_7360"] = df["beloep"].where((df["type"] == "3000"), 0)

# Avleder O_7362
df["O_7362"] = df["beloep"].where((df["type"] == "3100"), 0)

# Avleder O_7364
df["O_7364"] = df["beloep"].where((df["type"] == "3200"), 0)

# Avleder O_15843
df["O_15843"] = df["beloep"].where((df["type"] == "3300"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='annenDriftsinntekt'></a>
# ### 5. annenDriftsinntekt

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "annenDriftsinntekt"

# Leser inn skatteobjektet
df = spark.sql(annenDriftsinntekt).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7368
df["O_7368"] = df["beloep"].where((df["type"] == "3400"), 0)

# Avleder O_39729
df["O_39729"] = df["beloep"].where((df["type"] == "3410"), 0)

# Avleder O_7370
df["O_7370"] = df["beloep"].where((df["type"] == "3600"), 0)

# Avleder O_17163
df["O_17163"] = df["beloep"].where((df["type"] == "3650"), 0)

# Avleder O_32864
df["O_32864"] = df["beloep"].where((df["type"] == "3695"), 0)

# Avleder O_7374
df["O_7374"] = df["beloep"].where((df["type"] == "3700"), 0)

# Avleder O_737475, ny ORID
df["O_737475"] = df["beloep"].where((df["type"] == "3710"), 0)

# Avleder O_13676
df["O_13676"] = df["beloep"].where((df["type"] == "3890"), 0)

# Avleder O_7266
df["O_7266"] = df["beloep"].where((df["type"] == "3895"), 0)

# Avleder O_7376
df["O_7376"] = df["beloep"].where((df["type"] == "3900"), 0)

# Avleder O_37279, her summeres 3910 med 7911, siden før var disse rapportert netto (se RF-skjema)
# Ble ikke opprettet i NO-basen, summerers her for å ikke miste informasjon
df["O_37279"] = df["beloep"].where((df["type"] == "3910"), 0) + df["beloep"].where(
    (df["type"] == "7911"), 0
)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='varekostnad'></a>
# ### 6. varekostnad

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "varekostnad"

# Leser inn skatteobjektet
df = spark.sql(varekostnad).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7378
df["O_7378"] = df["beloep"].where((df["type"] == "4005"), 0)

# Avleder O_7380
df["O_7380"] = df["beloep"].where((df["type"] == "4295"), 0)

# Avleder O_7382
df["O_7382"] = df["beloep"].where((df["type"] == "4500"), 0)

# Avleder O_7384
df["O_7384"] = df["beloep"].where((df["type"] == "4995"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='loennskostnad'></a>
# ### 7. loennskostnad

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "loennskostnad"

# Leser inn skatteobjektet
df = spark.sql(loennskostnad).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_15844
df["O_15844"] = df["beloep"].where((df["type"] == "5000"), 0)

# Avleder O_15845
df["O_15845"] = df["beloep"].where((df["type"] == "5300"), 0)

# Avleder O_15846
df["O_15846"] = df["beloep"].where((df["type"] == "5400"), 0)

# Avleder O_7392
df["O_7392"] = df["beloep"].where((df["type"] == "5420"), 0)

# Avleder O_38841 skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_38841"] = df["beloep"].where((df["type"] == "5600"), 0)

# Avleder O_7396
df["O_7396"] = df["beloep"].where((df["type"] == "5900"), 0)

# Avleder O_27426
df["O_27426"] = df["beloep"].where((df["type"] == "5950"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='annenDriftskostnad'></a>
# ### 8. annenDriftskostnad

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "annenDriftskostnad"

# Leser inn skatteobjektet
df = spark.sql(annenDriftskostnad).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_15847
df["O_15847"] = df["beloep"].where((df["type"] == "6000"), 0)

# Avleder O_7400
df["O_7400"] = df["beloep"].where((df["type"] == "6100"), 0)

# Avleder O_15848
df["O_15848"] = df["beloep"].where((df["type"] == "6200"), 0)

# Avleder O_7404
df["O_7404"] = df["beloep"].where((df["type"] == "6300"), 0)

# Avleder O_7408
df["O_7408"] = df["beloep"].where((df["type"] == "6340"), 0)

# Avleder O_7406
df["O_7406"] = df["beloep"].where((df["type"] == "6395"), 0)

# Avleder O_15849
df["O_15849"] = df["beloep"].where((df["type"] == "6400"), 0)

# Avleder O_2812175, ny ORID, erstatter tidligere 6310, O_28121
df["O_2812175"] = df["beloep"].where((df["type"] == "6440"), 0)

# Avleder O_15850
df["O_15850"] = df["beloep"].where((df["type"] == "6500"), 0)

# Avleder O_7269
df["O_7269"] = df["beloep"].where((df["type"] == "6600"), 0)

# Avleder O_15836
df["O_15836"] = df["beloep"].where((df["type"] == "6695"), 0)

# Avleder O_7414
df["O_7414"] = df["beloep"].where((df["type"] == "6700"), 0)

# Avleder O_7416
df["O_7416"] = df["beloep"].where((df["type"] == "6995"), 0)

# Avleder O_3283675, ny ORID, tidligere post fra skjema 7098 O_32836, nå 6998
df["O_3283675"] = df["beloep"].where((df["type"] == "6998"), 0)

# Avleder O_7418
df["O_7418"] = df["beloep"].where((df["type"] == "7000"), 0)

# Avleder O_7422
df["O_7422"] = df["beloep"].where((df["type"] == "7020"), 0)

# Avleder O_15851
df["O_15851"] = df["beloep"].where((df["type"] == "7040"), 0)

# Avleder O_7273
df["O_7273"] = df["beloep"].where((df["type"] == "7080"), 0)

# Avleder O_15801
df["O_15801"] = df["beloep"].where((df["type"] == "7099"), 0)

# Avleder O_7424
df["O_7424"] = df["beloep"].where((df["type"] == "7155"), 0)

# Avleder O_7426
df["O_7426"] = df["beloep"].where((df["type"] == "7165"), 0)

# Avleder O_7428
df["O_7428"] = df["beloep"].where((df["type"] == "7295"), 0)

# Avleder O_7275
df["O_7275"] = df["beloep"].where((df["type"] == "7330"), 0)

# Avleder O_7277
df["O_7277"] = df["beloep"].where((df["type"] == "7350"), 0)

# Avleder O_727975, ny ORID, tidligere post 7495, O_7279, nå 7400
df["O_727975"] = df["beloep"].where((df["type"] == "7400"), 0)

# Avleder O_728075, ny post Gaver, fradragsberettiget
df["O_728075"] = df["beloep"].where((df["type"] == "7420"), 0)

# Avleder O_11334
df["O_11334"] = df["beloep"].where((df["type"] == "7500"), 0)

# Avleder O_7430
df["O_7430"] = df["beloep"].where((df["type"] == "7565"), 0)

# Avleder O_7432
df["O_7432"] = df["beloep"].where((df["type"] == "7600"), 0)

# Avleder O_15837
df["O_15837"] = df["beloep"].where((df["type"] == "7700"), 0)

# Avleder O_1202
df["O_1202"] = df["beloep"].where((df["type"] == "7890"), 0)

# Avleder O_3405675, ny ORID, tidligere post 7895, O_34056
df["O_3405675"] = df["beloep"].where((df["type"] == "7830"), 0)

# Avleder O_3405875, ny ORID, tidligere post 7896, O_34058
df["O_3405875"] = df["beloep"].where((df["type"] == "7860"), 0)

# Avleder O_7283
df["O_7283"] = df["beloep"].where((df["type"] == "7897"), 0)

# Avleder O_37281
df["O_37281"] = df["beloep"].where((df["type"] == "7910"), 0)

# Avleder O_791175
df["O_757911"] = df["beloep"].where((df["type"] == "7911"), 0)


# For 7911 se 3910 på driftsinntekter

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='finansinntekt'></a>
# ### 9. finansinntekt

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "finansinntekt"

# Leser inn skatteobjektet
df = spark.sql(finansinntekt).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_38885,
df["O_38885"] = df["beloep"].where((df["type"] == "8005"), 0)

# Avleder O_38887,
df["O_38887"] = df["beloep"].where((df["type"] == "8050"), 0)

# Avleder O_38888, Gevinst ved realisasjon av aksjer, egenkapitalbevis og fondsandeler
df["O_38888"] = df["beloep"].where((df["type"] == "8074"), 0)

# Avleder O_15852
df["O_15852"] = df["beloep"].where((df["type"] == "8060"), 0)

# Avleder O_15853, tidligere 8099
df["O_15853"] = df["beloep"].where((df["type"] == "8079"), 0)

# Avleder O_38889,
df["O_38889"] = df["beloep"].where((df["type"] == "8090"), 0)

# Avleder O_88888, 3 % av netto skattefrie inntekter etter fritaksmetoden og 3 % av utdeling fra selskap med deltakerfastsetting til selskapsdeltaker
df["O_88888"] = df["beloep"].where((df["type"] == "8091"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='finanskostnad'></a>
# ### 10. finanskostnad

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "finanskostnad"

# Leser inn skatteobjektet
df = spark.sql(finanskostnad).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_758105
df["O_758105"] = df["beloep"].where((df["type"] == "8105"), 0)

# Avleder O_38891
df["O_38891"] = df["beloep"].where((df["type"] == "8150"), 0)

# Avleder O_15854
df["O_15854"] = df["beloep"].where((df["type"] == "8160"), 0)

# Avleder O_38890
df["O_38890"] = df["beloep"].where((df["type"] == "8174"), 0)

# Avleder O_7441, tidligere post 8199
df["O_7441"] = df["beloep"].where((df["type"] == "8179"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='balanseverdiForAnleggsmiddel'></a>
# ### 11. balanseverdiForAnleggsmiddel

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "balanseverdiForAnleggsmiddel"

# Leser inn skatteobjektet
df = spark.sql(balanseverdiForAnleggsmiddel).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7445
df["O_7445"] = df["beloep"].where((df["type"] == "1000"), 0)

# Avleder O_2400
df["O_2400"] = df["beloep"].where((df["type"] == "1020"), 0)

# Avleder O_7447
df["O_7447"] = df["beloep"].where((df["type"] == "1080"), 0)

# Avleder O_15796
df["O_15796"] = df["beloep"].where((df["type"] == "1105"), 0)

# Avleder O_15795
df["O_15795"] = df["beloep"].where((df["type"] == "1115"), 0)

# Avleder O_36968
df["O_36968"] = df["beloep"].where((df["type"] == "1117"), 0)

# Avleder O_32838
df["O_32838"] = df["beloep"].where((df["type"] == "1120"), 0)

# Avleder O_7451
df["O_7451"] = df["beloep"].where((df["type"] == "1130"), 0)

# Avleder O_7454
df["O_7454"] = df["beloep"].where((df["type"] == "1150"), 0)

# Avleder O_7456
df["O_7456"] = df["beloep"].where((df["type"] == "1160"), 0)

# Avleder O_15792
df["O_15792"] = df["beloep"].where((df["type"] == "1205"), 0)

# Avleder O_15793
df["O_15793"] = df["beloep"].where((df["type"] == "1221"), 0)

# Avleder O_15794
df["O_15794"] = df["beloep"].where((df["type"] == "1225"), 0)

# Avleder O_15791
df["O_15791"] = df["beloep"].where((df["type"] == "1238"), 0)

# Avleder O_37283
df["O_37283"] = df["beloep"].where((df["type"] == "1239"), 0)

# Avleder O_15790
df["O_15790"] = df["beloep"].where((df["type"] == "1280"), 0)

# Avleder O_26535
df["O_26535"] = df["beloep"].where((df["type"] == "1290"), 0)

# Avleder O_7306
df["O_7306"] = df["beloep"].where((df["type"] == "1295"), 0)

# Avleder O_15838
df["O_15838"] = df["beloep"].where((df["type"] == "1296"), 0)

# Avleder O_37285
df["O_37285"] = df["beloep"].where((df["type"] == "1298"), 0)

# Avleder O_38915, 1350 Investeringer i aksjer, andeler og verdipapirfondsandeler
df["O_38915"] = df["beloep"].where((df["type"] == "1350"), 0)

# Avleder O_38916
# df["O_38916"] = df["beloep"].where((df["type"] == "1360"), 0)

df["O_38916"] = df["beloep"].where((df["type"] == "1360"), 0) + df["beloep"].where(
    (df["type"] == "1350"), 0
)

# Avleder O_38917
df["O_38917"] = df["beloep"].where((df["type"] == "1370"), 0)

# Avleder O_38918
df["O_38918"] = df["beloep"].where((df["type"] == "1380"), 0)

# Avleder O_338919
df["O_38919"] = df["beloep"].where((df["type"] == "1390"), 0)


# DE NYE POSTENE 1370, 1380 og 1390 skal brukes i år som omløpsmiddel på dette RF-1175 skjemaet
# siden disse ikke ble opprettet i NO-basen
# (SE POSTENE PÅ balanseverdiForOmloepsmiddel ned: O_7465, O_7467 og O_7469)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='balanseverdiForOmloepsmiddel'></a>
# ### 12. balanseverdiForOmloepsmiddel

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "balanseverdiForOmloepsmiddel"

# Leser inn skatteobjektet
df = spark.sql(balanseverdiForOmloepsmiddel).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_1576875, ny ORID, tidligere post 1495, O_15768 (test verdi her mot O_15768 sumVerdiAvVarelager,
# fra spesifikasjonAvVarelager på del 2)
df["O_1576875"] = df["beloep"].where((df["type"] == "1400"), 0)

# Avleder O_140175, ny ORID
df["O_140175"] = df["beloep"].where((df["type"] == "1401"), 0)

# Avleder O_18116
df["O_18116"] = df["beloep"].where((df["type"] == "1500"), 0)

# Avleder O_26537
df["O_26537"] = df["beloep"].where((df["type"] == "1530"), 0)

# Avleder O_38924, 1565 Kortsiktige fordringer mot personlig eier, styremedlem o.l.
df["O_38924"] = df["beloep"].where((df["type"] == "1565"), 0)

# Avleder O_38925
df["O_38925"] = df["beloep"].where((df["type"] == "1570"), 0)

# Merk at post 1595 med ORID 7310 forsvinner, så 7310 blir tom fra Sirius data i NO-basen for ENK

# Avleder O_7471
df["O_7471"] = df["beloep"].where((df["type"] == "1780"), 0)

# Avleder O_38927, 1800 Ikke-markedsbaserte aksjer og verdipapirfondsandeler
df["O_38927"] = df["beloep"].where((df["type"] == "1800"), 0)

# Avleder O_51810, 1810 markedsbaserte aksjer og verdipapirfondsandeler
df["O_51810"] = df["beloep"].where((df["type"] == "1810"), 0)

# Avleder O_38928 (tidligere 1869 levert som O_7473) 1830 Markedsbaserte obligasjoner, sertifikater mv.
df["O_38928"] = df["beloep"].where((df["type"] == "1830"), 0)

# Avleder O_7474, tidligere 1869
df["O_7474"] = df["beloep"].where((df["type"] == "1880"), 0)

# Avleder O_15789
df["O_15789"] = df["beloep"].where((df["type"] == "1895"), 0)

# Avleder O_15812
df["O_15812"] = df["beloep"].where((df["type"] == "1900"), 0)

# Avleder O_7477
df["O_7477"] = df["beloep"].where((df["type"] == "1920"), 0)

# Avleder O_7315
df["O_7315"] = df["beloep"].where((df["type"] == "1950"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='Egenkapital'></a>
# ### 13. Egenkapital

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "Egenkapital"

# Leser inn skatteobjektet
df = spark.sql(Egenkapital).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7479 MEN inkluderes også post 2000 Aksjekap./EK andre foretak (NY FOR REGNSKAPSPLIKTSTYPE 1)
# SUMMEN MÅ KONTROLLERES MOT DATA.

df["O_7479"] = df["beloep"].where((df["type"] == "2015"), 0)

# Avleder O_7481
df["O_7481"] = df["beloep"].where((df["type"] == "2050"), 0)

# Avleder O_7483
df["O_7483"] = df["beloep"].where((df["type"] == "2080"), 0)

# Avleder O_1471
df["O_1471"] = df["beloep"].where((df["type"] == "2095"), 0)

# Avleder O_15839
df["O_15839"] = df["beloep"].where((df["type"] == "2096"), 0)

# Avleder O_15803
df["O_15803"] = df["beloep"].where((df["type"] == "2097"), 0)

# Avleder O_37287
df["O_37287"] = df["beloep"].where((df["type"] == "2098"), 0)

# Avleder O_38936
df["O_38936"] = df["beloep"].where((df["type"] == "2000"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='LangsiktigGjeld'></a>
# ### 14. LangsiktigGjeld

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "LangsiktigGjeld"

# Leser inn skatteobjektet
df = spark.sql(LangsiktigGjeld).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7485
df["O_7485"] = df["beloep"].where((df["type"] == "2220"), 0)

# Avleder O_7327, tidligere post 2275
df["O_7327"] = df["beloep"].where((df["type"] == "2290"), 0)

# Avleder O_7487, tidligere post 2289
df["O_7487"] = df["beloep"].where((df["type"] == "2280"), 0) + df["beloep"].where(
    (df["type"] == "2289"), 0
)


# Avleder O_38945
df["O_38945"] = df["beloep"].where((df["type"] == "2250"), 0)

# Avleder O_38947
df["O_38947"] = df["beloep"].where((df["type"] == "2290"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='KortsiktigGjeld'></a>
# ### 15. KortsiktigGjeld

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "KortsiktigGjeld"

# Leser inn skatteobjektet
df = spark.sql(KortsiktigGjeld).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_7489
df["O_7489"] = df["beloep"].where((df["type"] == "2380"), 0)

# Avleder O_7491
df["O_7491"] = df["beloep"].where((df["type"] == "2400"), 0)

# Avleder O_7493
df["O_7493"] = df["beloep"].where((df["type"] == "2600"), 0)

# Avleder O_7495
df["O_7495"] = df["beloep"].where((df["type"] == "2740"), 0)

# Avleder O_7497
df["O_7497"] = df["beloep"].where((df["type"] == "2770"), 0)

# Avleder O_7499
df["O_7499"] = df["beloep"].where((df["type"] == "2790"), 0)

# Avleder O_29065
df["O_29065"] = df["beloep"].where((df["type"] == "2800"), 0)

# Avleder O_7501
df["O_7501"] = df["beloep"].where((df["type"] == "2900"), 0)

# Avleder O_7503
df["O_7503"] = df["beloep"].where((df["type"] == "2910"), 0)

# Avleder O_7505
df["O_7505"] = df["beloep"].where((df["type"] == "2949"), 0)

# Avleder O_7507
df["O_7507"] = df["beloep"].where((df["type"] == "2950"), 0)

# Avleder O_750975, ny ORID, tidligere 2995, O_7509
df["O_750975"] = df["beloep"].where((df["type"] == "2990"), 0)

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Printer oppsummerende statistikk:
df.describe().transpose()
# -

# <a id='sum_resultatregnskap'></a>
# ### 16. sum_resultatregnskap

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "sum_resultatregnskap"

# Leser inn skatteobjektet
df = spark.sql(sum_resultatregnskap).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_15799, Sum driftsinntekt
df["O_15799"] = df["sumDriftsinntekt"]

# Avleder O_7286, Sum driftskostnad
df["O_7286"] = df["sumDriftskostnad"]

# Avleder O_6686, Driftsresultat
df["O_6686"] = df["sumDriftsinntekt"] - df["sumDriftskostnad"]

# Avleder O_13962, Sum finansinntekter
df["O_13962"] = df["sumFinansinntekt"]

# Avleder O_13964, Sum finanskostnader
df["O_13964"] = df["sumFinanskostnad"]

# Avleder O_6675, Resultat
df["O_6675"] = df["aarsresultat"]

# Avleder O_28070. Piloten gjelder kun enkeltpersonsforetak dermed vil alt
# overføres fra O_6675, se s.2 NO1 post 9930. (se post fra skjema 0401 resultat rad 240 i mapping excel
# skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_28070"] = df["aarsresultat"]

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Oppsummerende statistikk
df.describe().transpose()
# -

# <a id='sum_balanseregnskap'></a>
# ### 17. sum balanseregnskap

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "sum_balanseregnskap"

# Leser inn skatteobjektet
df = spark.sql(sum_balanseregnskap).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df)

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# sumBalanseverdiForAnleggsmiddel og sumBalanseverdiForOmloepsmiddel eksisterer ikke i NO-basen, men kan brukes for å kontrollere
# Avleder O_NY_ORID_sum_bal1, Ny post 9300 Sum anleggsmidler
# df['O_NY_ORID_sum_bal1'] = df['sumBalanseverdiForAnleggsmiddel']
# Avleder O_NY_ORID_sum_bal2, Ny post 9350 Sum omløpsmidler
# df['O_NY_ORID_sum_bal2'] = df['sumBalanseverdiForOmloepsmiddel']

# Avleder O_15797, Ny post 9400 Sum Eiendeler
df["O_15797"] = df["sumBalanseverdiForEiendel"]

# Avleder O_7318, Sum skattemessig egenkapital post 9970 MÅ SJEKKES OM DET KOMMER DIREKTE SOM SKATTEMESSIG
# df["O_7318"] = df["sumEgenkapital"]

# Avleder O_7324, post 9970 Sum ubeskattet egenkapital MÅ SJEKKES OM DET KOMMER DIREKTE SOM UBESKATTET
# df["O_7324"] = df["sumEgenkapital"]

# Avleder O_75250, post 9450 Sum ubeskattet egenkapital MÅ SJEKKES OM DET KOMMER DIREKTE SOM UBESKATTET
df["O_75250"] = df["sumEgenkapital"]

# sumLangsiktigGjeld og sumKortsiktigGjeld eksisterer ikke i NO-basen, men kan brukes for å kontrollere
# Avleder O_NY_ORID_sum_bal3, ny post 9500 Sum Langsiktig Gjeld
# df['O_NY_ORID_sum_bal3'] = df['sumLangsiktigGjeld']
# Avleder O_NY_ORID_sum_bal4, ny post 9550 Sum kortsiktig gjeld
# df['O_NY_ORID_sum_bal4'] = df['sumKortsiktigGjeld']

# Avleder O_15855, post 9990 Sum gjeld      MÅ SEES OM DET KOMMER DIREKTE OGSÅ
# df["O_15855"] = df["sumLangsiktigGjeld"] + df["sumKortsiktigGjeld"]

# Avleder O_38948, post 9500 Sum langsiktig gjeld
df["O_38948"] = df["sumLangsiktigGjeld"]

# Avleder O_38958, post 9550 Sum kortsiktig gjeld
df["O_38958"] = df["sumKortsiktigGjeld"]

# Avleder O_7511, post 9995 SUM EGENKAPITAL OG GJELD
df["O_7511"] = df["sumGjeldOgEgenkapital"]

# Avleder O_38920
df["O_38920"] = df["sumBalanseverdiForAnleggsmiddel"]

# Avleder O_38934
df["O_38934"] = df["sumBalanseverdiForOmloepsmiddel"]

df["kontroll"] = df["sumBalanseverdiForEiendel"] - df["sumGjeldOgEgenkapital"]


# Egenkapital 31.12.2020
# Midlertidige forskjeller 31.12.2020, jf. RF-1217
# Korrigert egenkapital 1.1.2021

# SIDE 4 fra skjema trenger vi ikke å kode siden vi bruker RF-1052 i stedet for det.

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Oppsummerende statistikk
df.describe().transpose()
# -

df.sort_values(by=["kontroll"], ascending=False)

# <a id='fordelt_beregnet_naeringsinntekt'></a>
# ### 22. fordelt beregnet naeringsinntekt

# +
# Definerer hvilket skatteobjekt som skal leses inn
df_name = "fordeltBeregnetNaeringsinntekt"

# Leser inn skatteobjektet
df = spark.sql(fordeltBeregnetNaeringsinntekt).to_pandas_on_spark()

# Filtrerer på regnskapspliktstype
df = df[
    df["virksomhetstype"].isin(
        ["enkeltpersonforetak", "selskapMedDeltakerfastsetting", "samvirkeforetak"]
    )
    & df["regnskapspliktstype"].isin(["ingenRegnskapsplikt", "begrensetRegnskapsplikt"])
]

# Fyller integers med 0 og booleans med False
df = functions.fillna(df, include=["number", "bool", "string"])

# Lager liste med variablene som er i skatteobjektetet ved innlesing
remove_cols = [col for col in df.columns if col != "orgnr"]

# Avleder O_19797 skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_19797"] = df["fordeltSkattemessigResultatEtterKorreksjon"]

# Avleder O_19798 skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_19798"] = (
#     df["fordeltSkattemessigResultatEtterKorreksjon"]
#     * (df["andelAvFordeltSkattemessigResultatTilordnetInnehaver"] / 100)
# ).fillna(0)

# Avleder O_19799 skal ikke med i 1175. Det er data som ikke finnes i NO basen fra før og noen har utgått.
# df["O_19799"] = df["O_19797"] - df["O_19798"]

# Lager datasett med variabler som skal være med i skjemaet
df = df.drop(remove_cols, axis=1)

# Aggregerer beløp i avledet skatteobjekt
df = df.groupby("orgnr", as_index=False).sum()

# Lagrer avledet skatteobjekt på DAPLA:
# df.to_parquet(f"{TEMP}/RF1175_{tverrsnitt}_{tverrsnitt_versjon}/{df_name}")

# Oppsummerende statistikk
df.describe().transpose()
