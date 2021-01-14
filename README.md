# Supinfo Analytics - Spark job

Ce dépôt est un projet Spark pour l'application **Supinfo Analytics**, 
visant à récupérer, agréger, dénormaliser et stocker les données sur
les étudiants, leurs notes, les contrats de professionalisation, les 
entreprises qui embauchent et les étudiants qui arrêtent leur cursus 
à Supinfo en cours de route.

## Bibliothèques utilisées

- Scala 2.11.12
- Spark 2.4.6 :
  - Spark SQL
  - Spark Avro
  - Spark Kafka
  - Cassandra connector 2.5.1 (de Datastax)
- Confluent 6.0.1 :
  - Schema registry client
  - Avro SerDe
  
## Jobs implémentés

### StudentCurriculumOverviewSparkApplication

Ce job permet d'agréger les informations personnelles et scolaires
d'un étudiant avec les notes qu'il a eues tout au long de sa scolarité.

#### Informations

Les données prises en entrées sont :
- **Étudiants** : nom, prénom, âge, genre, adresse mail scolaire, 
  téléphone personnel, campus d'origine ;
- **Notes** : année du cours (1-5), catégorie du cours, note obtenue ;
- **Contrat pro** : nom de l'entreprise et date de début du contrat (si
  l'étudiant est actuellement en contrat pro).
  
Les données sont agrégées et stockées dans les tables suivantes :

- **`supinfodwh.student_notes_by_course_and_promotion`** : partitionnement 
  par élèves, regroupées par promotion, catégorie de cours puis 
  note obtenue.
- **`supinfodwh.student_notes_by_region`** : partitionnement
  par région, regroupées par campus puis par note obtenue.

### FormerStudentSparkApplication

Ce job permet de regrouper les informations des étudiants ayant quitter
Supinfo pendant leur cursus scolaire, ainsi que celle de l'école qu'ils
ont rejoint au profit de Supinfo.

#### Informations

Les données prises en entrées sont :
- **Étudiants** : nom, prénom, âge, genre, adresse mail scolaire,
  téléphone personnel, campus d'origine ;
- **Concurrents** : nom de l'école concurrent vers laquelle l'élève
  s'est tourné au profit de Supinfo ;
- **Notes** : année, moyenne générale ;
- **Contrat pro** : nom de l'entreprise et date de début du contrat (si
  l'étudiant est actuellement en contrat pro).

Les données sont aggrégées et stockées dans la table 
**`supinfodwh.former_students_by_concurrents`**, partitionnées par
élèves et regroupées par concurrent puis par moyenne générale.

### HiredStudentSparkApplication

Ce job permet de regrouper les informations des étudiants ayant été
embauché suite à leur cursus scolaire à Supinfo, ainsi que des
entreprises recruteuses.

#### Informations

Les données prises en entrées sont :
- **Étudiants** : nom, prénom, âge, genre, adresse mail scolaire,
  téléphone personnel, campus d'origine ;
- **Contrats** : nom de l'entreprise qui a recruté l'élève et date
  de début du contrat ;
- **Notes** : année, moyenne générale ;
- **Contrat pro** : nom de l'entreprise et date de début du contrat (si
  l'étudiant est actuellement en contrat pro).

Les données sont aggrégées et stockées dans la table
**`supinfodwh.hired_students_by_company`**, partitionnées par
élèves et regroupées par entreprise recruteuse puis par moyenne 
générale.

## Développement

### Prérequis

- Java 8+
- Scala 2.11
- Maven

### Build

Lancer la commande suivante à la racine du projet :

```shell
mvn clean package
```
