pipeline {
    environment {
        RELEASE_NAME = "appserver"
        IMAGE_TAG_PREFIX = "3.0.${BUILD_NUMBER}-"
        
        GCP_PROJECT_ID = credentials('GCP_PROJECT_ID')
        PROD_IMAGE = "us-docker.pkg.dev/${GCP_PROJECT_ID}/e6-engine/${RELEASE_NAME}"

        TRIVY_VERSION = "v0.56.2"
        TRIVY_OPTIONS = "--db-repository public.ecr.aws/aquasecurity/trivy-db"
    }

    options {
        skipDefaultCheckout()
        disableConcurrentBuilds()
    }

    agent {
        kubernetes {
            defaultContainer 'jnlp'
        }
    }

    stages {
         stage('Sonarqube Scan') {
      agent {
                kubernetes {
                    inheritFrom 'docker'
                    defaultContainer 'docker'
                }
            }

      environment {
              SCANNER_HOME = tool 'sonarqube'
            }

      steps {
        withSonarQubeEnv('sonarqube-jenkins') {
            checkout scm
            sh '${SCANNER_HOME}/bin/sonar-scanner'
        }
      }
    }

     // stage("Quality Gate") {
    //   steps {
    //     timeout(time: 1, unit: 'HOURS') {
    //       waitForQualityGate abortPipeline: true
    //     }
    //   }
    // }

    stage ('Trivy Scan') {
      agent {
        kubernetes {
          inheritFrom 'pythondarm'
          defaultContainer 'python'
        }
      }

      steps {
          checkout scm
          sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
          sh 'mkdir -p /tmp/trivy/'
          sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
          sh 'tar -xvf /tmp/trivy/db.tar.gz'
          script {
              def trivyResult = sh(
                  script: "trivy fs --exit-code 1 --cache-dir='/tmp/trivy' ${TRIVY_OPTIONS} --no-progress --scanners vuln,misconfig .",
                  returnStatus: true
              )
              if (trivyResult == 0) {
                  // Trivy scan passed, push the image
                  sh 'echo "Trivy scan passed for Python Scripts."'
                  }
              else {
                  error('Trivy scan failed')
              }
          }
      }
    }

    stage ('Cloud Authentication') {
      agent {
            kubernetes {
                inheritFrom 'cloud'
                defaultContainer 'cloud'
            }
          }

      environment {
        GCP_SA_PATH = credentials('JENKINS_GCP_SA')
      }    

      steps {
        checkout scm
        sh 'git config --global --add safe.directory "*"'
        sh 'cp ${GCP_SA_PATH} hello.json && gcloud auth activate-service-account --key-file=hello.json'
        sh 'gcloud config set project ${GCP_PROJECT_ID}'
        sh 'gcloud auth configure-docker us-central1-docker.pkg.dev'
        script {
          env.GIT_COMMIT_HASH = sh (script: 'git rev-parse --short HEAD', returnStdout: true)
          env.TAG_VALUE = "${IMAGE_TAG_PREFIX}${GIT_COMMIT_HASH}"
          env.GCP_DOCKER_TOKEN=sh(returnStdout: true, script: "gcloud auth print-access-token").trim() 
        }
      }    
    }

    stage('Production builds') {
      parallel {
        stage('ARM builds') {
          agent {
            kubernetes {
              inheritFrom 'docker'
              defaultContainer 'docker'
            }
          }
      
          steps {
            checkout scm
            sh 'docker login -u oauth2accesstoken -p ${GCP_DOCKER_TOKEN} https://us-docker.pkg.dev'
            sh 'docker buildx create --name mybuilder  --use --platform linux/arm64,linux/amd64'
            sh 'mkdir -p /tmp/trivy/'
            sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
            sh 'tar -xvf /tmp/trivy/db.tar.gz'
            sh 'docker buildx build --no-cache --platform linux/arm64  -t ${RELEASE_NAME} --load .'
            sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
            script {
            def trivyResult = sh(
                script: "trivy image --exit-code 1 --cache-dir='/tmp/trivy' ${TRIVY_OPTIONS} --no-progress --scanners vuln,misconfig,secret ${RELEASE_NAME}",
                returnStatus: true
            )
            if (trivyResult == 0) {
            // Trivy scan passed, push the image
            sh 'docker buildx build --platform linux/arm64 -t ${PROD_IMAGE} --push --output=type=image,push-by-digest=true --metadata-file meta-arm64.json .'
            env.ARM_HASH = sh(returnStdout: true, script: "cat meta-arm64.json | jq -r '.\"containerimage.digest\"'").trim()
            }
            else {
            error('Trivy scan failed for Docker image. Image will not be pushed.')
            }
            }   
          }
        }

        stage('AMD builds') {
          agent {
            kubernetes {
              inheritFrom 'docker-amd'
              defaultContainer 'docker'
            }
          }
      
          steps {
            checkout scm
            sh 'docker login -u oauth2accesstoken -p ${GCP_DOCKER_TOKEN} https://us-docker.pkg.dev'
            sh 'docker buildx create --name mybuilder  --use --platform linux/arm64,linux/amd64'
            sh 'mkdir -p /tmp/trivy/'
            sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
            sh 'tar -xvf /tmp/trivy/db.tar.gz'
            sh 'docker buildx build --no-cache --platform linux/amd64  -t ${RELEASE_NAME} --load .'
            sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
            script {
            def trivyResult = sh(
                script: "trivy image --exit-code 1 --cache-dir='/tmp/trivy' ${TRIVY_OPTIONS} --no-progress --scanners vuln,misconfig,secret ${RELEASE_NAME}",
                returnStatus: true
            )
            if (trivyResult == 0) {
            // Trivy scan passed, push the image
            sh 'docker buildx build --platform linux/amd64 -t ${PROD_IMAGE} --push --output=type=image,push-by-digest=true --metadata-file meta-amd64.json .'
            env.AMD_HASH = sh(returnStdout: true, script: "cat meta-amd64.json | jq -r '.\"containerimage.digest\"'").trim()
            }
            else {
            error('Trivy scan failed for Docker image. Image will not be pushed.')
            }
            }   
          }
        }
      }
    }

    stage('Prod artifacts- push') {
      agent {
        kubernetes {
          inheritFrom 'docker'
          defaultContainer 'docker'
        }
      }

      steps {
        sh 'docker login -u oauth2accesstoken -p ${GCP_DOCKER_TOKEN} https://us-docker.pkg.dev'
        sh 'docker buildx imagetools create  --tag ${PROD_IMAGE}:${TAG_VALUE} ${AMD_HASH} ${ARM_HASH}'
      }
    }
  }
}
