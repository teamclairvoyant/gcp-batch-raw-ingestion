def userId = slackUserIdFromEmail("${BUILD_USER_EMAIL}")
pipeline {
  agent any

  parameters {

  choice(name:'Airflow_Dag_Upload',choices:['NONE','gcp-batch-raw-ingestion-dataflow','spark-scala-etl'],description:'Which Dag should be uploaded')
  choice(name:'Dataflow_Template_Build',choices:['NONE','source_to_gcs','gcs_to_bq'],description:'Which template should be build')
  choice(name:'Run_Airflow_Dag',choices:['NONE','gcp-batch-raw-ingestion-dataflow'],description:'Which Dag should be triggered')

  }

  stages {
    stage('List files from github') {
      steps {
        slackSend color: 'good', message: "Hi <@$userId> your build has started and url is ${env.BUILD_URL}"
        sh 'ls  -ltrh /bitnami/jenkins/home/workspace/gcp-batch-raw-ingestion-dataflow/dataflow/'
      }
    }

    stage('Uploading airflow Dags') {
        steps {
        script {
        if(params.Airflow_Dag_Upload != "NONE"){
        slackSend color: 'good', message: "Hi <@$userId> airflow dags are deployed"
        sh 'gsutil cp /bitnami/jenkins/home/workspace/gcp-batch-raw-ingestion-dataflow/airflow_dags/'+ params.Airflow_Dag_Upload +'.py  gs://us-central1-data-generator--fc43a156-bucket/dags'
        }
        }
      }
    }


    stage('Build the template and deploy on gcs') {
      steps {
      script {
      if(params.Dataflow_Template_Build != "NONE"){
      echo 'Running requirements.txt'
      slackSend color: 'good', message: "Hi <@$userId> Running requirements.txt "
       sh 'pip3 install -r /bitnami/jenkins/home/workspace/gcp-batch-raw-ingestion-dataflow/dataflow/requirements.txt --user'

      echo 'Running setup.py'
      dir('/bitnami/jenkins/home/workspace/dataflow-etl/dataflow/'){
          slackSend color: 'good', message: "Hi <@$userId> Running setup.py"
          sh 'python3 /bitnami/jenkins/home/workspace/gcp-batch-raw-ingestion-dataflow/dataflow/setup.py install --user'
		  }

      echo 'Building the template and deploying on gcs'
      dir('/bitnami/jenkins/home/workspace/gcp-batch-raw-ingestion-dataflow/dataflow/'){
         slackSend color: 'good', message: "Hi <@$userId> build the template and deploy on gcs"
         sh 'python3 -m gcs_to_bq --runner DataflowRunner  --project playground-375318  --staging_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/staging  --temp_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/temp --template_location gs://bronze-poc-group/gcp-batch-raw-ingestion/dataflow/templates/gcs_to_bq --region US --config_file config/gcs_to_bq_config.json --setup_file ./setup.py --save_main_session'
         slackSend color: 'good', message: "Hi <@$userId> templated is deployed "
      }
      }

       }
      }
    }
    stage('Running Dag with airflow') {
       steps {
       script {
       if(params.Run_Airflow_Dag != "NONE"){
       withEnv(['GCLOUD_PATH=/usr/lib/google-cloud-sdk/bin']) {
                slackSend color: 'good', message: "Running Dag gcp-batch-raw-ingestion-dataflow with airflow"
                sh '$GCLOUD_PATH/gcloud composer environments  run  data-generator-demo --location us-central1  dags trigger -- gcp-batch-raw-ingestion-dataflow'

       }
       }

             }
         }
    }
  }
  post {
        success {
             echo 'success'
             slackSend color: 'good', message: "Hi <@$userId> Airflow dag is triggered please check the ui"
        }
        failure {
              echo 'failure'
              slackSend color: 'danger', message: "Hi <@$userId> your build has failed pleas check ${env.BUILD_URL}"
                }
        }

}