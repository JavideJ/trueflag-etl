<h1 align="center">trueflag-etl</h1>
Proyecto para realizar una ETL desde un bucket de s3 hasta BigQuery. Este incluye no solo el código para realizar la extracción, transformación y carga, sino también la infraestructura como código para su despliegue en GCP mediante terraform.<br>
La solución se podrá lanzar tando desde local mediante el main.py como desde GCP por medio de la Cloud Function que se despliega.<br>
En la carpeta doc se puede ampliar información sobre la arquitectura y el diseño de la solución.

## Prerrequisitos
Para poder hacer uso de este proyecto, primeri se necesita tener instalado:<br>
- [python]('https://www.python.org/downloads/') - versión 3.10
- [git]('https://git-scm.com/downloads') - para clonar el repo
- [terraform]('https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli') - para el despliegue de la infraestructura en Google Cloud
- Además, es necesario tener una cuenta en Google Cloud con la **facturación habilitada**.

# Pasos a seguir para desplegar el proyecto
1. Configura una [cuenta de servicio]('https://console.cloud.google.com/iam-admin/serviceaccounts') de GCP con los permisos necesarios (para no perder tiempo con esta parte, se puede otorgar un rol con todos los permisos, como el rol de owner).<br><br>
2. Crea y descarga las credenciales de ese cuenta en formato json (ver esta [guía]('https://cloud.google.com/iam/docs/keys-create-delete?hl=es-419#iam-service-account-keys-create-console'))<br><br>
3. Clona el repo en tu local. Al tratarse de un repo privado, sólo aquellos que tengan acceso (o dispongan de un token) podrán hacerlo. Desde una terminal ejecuta:<br>
```bash
git clone https://github.com/JavideJ/trueflag-etl.git
```
5. Sutituye los archivos cred.json que encontrarás en las carpetas **cloud_function/** y **terraform/** por las credenciales de tu cuenta de servicio.<br><br>
6. En el archivo config.yaml y cloud_function/config.yaml cambia el valor de **project_id** por el de tu proyecto de GCP.<br><br>
7. En el archivo terraform/terraform.tfvars cambia las variables **project_id** y **service_account** por las tuyas y la variable **bucket_name** por un nombre de bucket nuevo, que no exista.<br><br>
8. Comprime todos los archivos de la carpeta cloud_function en un archivo zip y guárdalo dentro de la carpeta cloud_function. Este archivo comprimido es el que se usará para desplegar la función en GCP.
   Después de esto ya tenemos todo lo necesario para iniciar el despliegue:<br><br>
9. Desde la terminal ejecuta los siguientes comandos desde la raíz del proyecto:<br>
```bash
cd terraform
terraform init
terraform apply
```
Esto desplegará la infraestructura necesaria en Google Cloud, incluída la Cloud Function que realizará la ETL.

# Lanzar la ETL
Tenemos dos posibilidades:
1. Desde local, ejecutando el main.py (instala antes las librerías del requirements.txt). En el config.yaml podemos realizar algunos ajustes, como la fecha de los datos que queremos subir.<br><br>
2. Desde GCP. Lo más fácil es ir a Cloud Scheduler, activar la programación de la función y darle a 'forzar ejecución'. Desde los logs de 'Funciones de Cloud Run' podemos ir viendo en directo los mensajes que se imprimen.
