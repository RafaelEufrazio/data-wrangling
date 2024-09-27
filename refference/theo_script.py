import pandas as pd

# Open file
df = pd.read_csv(filepath_or_buffer="./microdados_ed_basica_2023.csv", sep=';', decimal=',')
print("TOTAL SIZE:  " + len(df).__str__())

# Remove lines where TP_DEPENDENCIA is not 4
df = df.drop(df[df['TP_DEPENDENCIA'] != 4].index)
print("AFTER TP_DEPENDENCIA SIZE:  " + len(df).__str__())

# Remove lines where TP_SITUACAO_FUNCIONAMENTO is not 1
df = df.drop(df[df['TP_SITUACAO_FUNCIONAMENTO'] != 1].index)
print("AFTER TP_SITUACAO_FUNCIONAMENTO SIZE:  " + len(df).__str__())

# Remove useless columns
AGUA = ["IN_AGUA_POTAVEL", "IN_AGUA_REDE_PUBLICA", "IN_AGUA_POCO_ARTESIANO", "IN_AGUA_CACIMBA", "IN_AGUA_FONTE_RIO", "IN_AGUA_INEXISTENTE"]
ESGOTO = ["IN_ESGOTO_REDE_PUBLICA", "IN_ESGOTO_FOSSA_SEPTICA", "IN_ESGOTO_FOSSA_COMUM", "IN_ESGOTO_FOSSA", "IN_ESGOTO_INEXISTENTE"]
PROF = ["IN_DORMITORIO_PROFESSOR", "IN_SALA_PROFESSOR"]
df = df.drop([*AGUA, *ESGOTO, *PROF], axis=1)
print("AFTER REMOVING COLUMNS SIZE:  " + len(df).__str__())

# Save new file
df.to_csv("microdados_ed_basica_2023_TRATADO.csv", decimal=',', sep=';')

# IN_AGUA_POTAVEL, IN_AGUA_REDE_PUBLICA, IN_AGUA_POCO_ARTESIANO, IN_AGUA_CACIMBA, IN_AGUA_FONTE_RIO, IN_AGUA_INEXISTENTE
# IN_ESGOTO_REDE_PUBLICA, IN_ESGOTO_FOSSA_SEPTICA, IN_ESGOTO_FOSSA_COMUM, IN_ESGOTO_FOSSA, IN_ESGOTO_INEXISTENTE
# IN_DORMITORIO_PROFESSOR, IN_SALA_PROFESSOR