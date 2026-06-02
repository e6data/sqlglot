pipeline {
  environment {
    IMAGE_TAG_PREFIX = "1.0.${BUILD_NUMBER}-"
    PR_TAG_PREFIX = "0.2.${BUILD_NUMBER}-pr-"

    REPOSITORY = "github.com/e6data/e6-transpiler"
    RELEASE_NAME = "transpiler"
    HUB_IMAGE = "e6data/${RELEASE_NAME}"
    HUB_TOKEN = credentials('HUB_TOKEN')

    AWS_REGION = "us-east-1"
    ASSUMED_ROLE_ARN ="arn:aws:iam::670514002493:role/cross-account-jenkins-access"

    GCP_PROJECT_ID = credentials('GCP_PROJECT_ID')

    PROD_IMAGE = "us-docker.pkg.dev/${GCP_PROJECT_ID}/e6-engine/${RELEASE_NAME}"
    DEV_IMAGE = "670514002493.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"

    // Additional registries
    ACR_IMAGE = "e6labs.azurecr.io/e6-engine/${RELEASE_NAME}"
    SERVERLESS_BETA_IMAGE = "908027423391.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"
    SERVERLESS_PROD_IMAGE = "390844744777.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"

    // Role ARNs for cross-account access
    SERVERLESS_BETA_ROLE_ARN = "arn:aws:iam::908027423391:role/cross-account-jenkins-access"
    SERVERLESS_PROD_ROLE_ARN = "arn:aws:iam::390844744777:role/cross-account-jenkins-access"

    JMX_PROM_VERSION = "1.0.1"
  }

  agent {
    kubernetes {
      defaultContainer 'jnlp'
    }
  }

  options {
    skipDefaultCheckout()
    disableConcurrentBuilds()
  }

  stages{
    stage('Initial Setup') {
      parallel {
        stage('aws auth') {
          agent {
            kubernetes {
              inheritFrom 'helmdeploy'
              defaultContainer 'helm'
            }
          }

          steps {
            sh 'apt install jq -y'
            checkout scm
            sh 'git config --global --add safe.directory "*"'
            sh 'aws s3 cp s3://e6-maven-repo/version.py .'
            sh 'pip3 install boto3'
            script {
              env.GIT_COMMIT_HASH = sh (script: 'git rev-parse --short HEAD', returnStdout: true).trim()
              // env.TAG_VALUE = "${IMAGE_TAG_PREFIX}${GIT_COMMIT_HASH}"
               if ( env.BRANCH_NAME == 'main' ) {
                env.TAG_VALUE = sh (returnStdout: true, script: 'echo ${IMAGE_TAG_PREFIX}${GIT_COMMIT_HASH}').trim()
              }
              else {
              env.TAG_VALUE = sh (returnStdout: true, script: 'echo ${PR_TAG_PREFIX}${GIT_COMMIT_HASH}').trim()
              }

              env.CODEARTIFACT_AUTH_TOKEN=sh(returnStdout: true, script: "aws codeartifact get-authorization-token --domain e6-labs --domain-owner 298655976287 --query authorizationToken --output text --region us-east-2").trim()

              env.THRIFT_NEXUS_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-services-thrift 1.0').trim()
              env.WRK_THRIFT_NEXUS_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-workspace-services-thrift 1.0').trim()
              env.CLOUD_UTILS_NEXUS_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-cloud-utils 1.0').trim()
              env.COMMON_NEXUS_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-common 1.0').trim()
              env.AUTH_NEXUS_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-auth-interface 1.0').trim()
              env.DELTA_LOG_READER_VERSION = sh (returnStdout: true, script: 'python3 version.py e6-delta-log-reader 1.0').trim()
              env.TEMP_ROLE=sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${ASSUMED_ROLE_ARN} --role-session-name transpiler-${BUILD_NUMBER}').trim()
              env.AWS_ACCESS_KEY_ID=sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.AccessKeyId"').trim()
              env.AWS_SECRET_ACCESS_KEY=sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.SecretAccessKey"').trim()
              env.AWS_SESSION_TOKEN=sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.SessionToken"').trim()
              env.ECR_TOKEN=sh(returnStdout: true, script: "aws ecr get-login-password --region ${AWS_REGION} --output text").trim()
            }
          }
        }

        stage('gcloud auth') {
          agent {
            kubernetes {
              inheritFrom 'gcloud'
              defaultContainer 'gcloud'
            }
          }

          environment {
            GCP_SA_PATH = credentials('JENKINS_GCP_SA')
          }

          steps {
            sh 'cp ${GCP_SA_PATH} hello.json && gcloud auth activate-service-account --key-file=hello.json'
            sh 'gcloud config set project ${GCP_PROJECT_ID}'
            sh 'gcloud auth configure-docker us-central1-docker.pkg.dev'
            script {
              env.GCP_DOCKER_TOKEN=sh(returnStdout: true, script: "gcloud auth print-access-token").trim()
            }
          }
        }

        stage('additional aws auth') {
          agent {
            kubernetes {
              inheritFrom 'helmdeploy'
              defaultContainer 'helm'
            }
          }

          steps {
            sh 'apt install jq -y'
            script {
              // Get Serverless Beta ECR token
              env.SERVERLESS_BETA_TEMP_ROLE=sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${SERVERLESS_BETA_ROLE_ARN} --role-session-name transpiler-slbeta-${BUILD_NUMBER}').trim()
              env.SERVERLESS_BETA_AWS_ACCESS_KEY_ID=sh(returnStdout: true, script: 'echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.AccessKeyId"').trim()
              env.SERVERLESS_BETA_AWS_SECRET_ACCESS_KEY=sh(returnStdout: true, script: 'echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.SecretAccessKey"').trim()
              env.SERVERLESS_BETA_AWS_SESSION_TOKEN=sh(returnStdout: true, script: 'echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.SessionToken"').trim()
              env.SERVERLESS_BETA_ECR_TOKEN=sh(returnStdout: true, script: "AWS_ACCESS_KEY_ID=${SERVERLESS_BETA_AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${SERVERLESS_BETA_AWS_SECRET_ACCESS_KEY} AWS_SESSION_TOKEN=${SERVERLESS_BETA_AWS_SESSION_TOKEN} aws ecr get-login-password --region ${AWS_REGION} --output text").trim()

              // Get Serverless Prod ECR token
              env.SERVERLESS_PROD_TEMP_ROLE=sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${SERVERLESS_PROD_ROLE_ARN} --role-session-name transpiler-slprod-${BUILD_NUMBER}').trim()
              env.SERVERLESS_PROD_AWS_ACCESS_KEY_ID=sh(returnStdout: true, script: 'echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.AccessKeyId"').trim()
              env.SERVERLESS_PROD_AWS_SECRET_ACCESS_KEY=sh(returnStdout: true, script: 'echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.SecretAccessKey"').trim()
              env.SERVERLESS_PROD_AWS_SESSION_TOKEN=sh(returnStdout: true, script: 'echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.SessionToken"').trim()
              env.SERVERLESS_PROD_ECR_TOKEN=sh(returnStdout: true, script: "AWS_ACCESS_KEY_ID=${SERVERLESS_PROD_AWS_ACCESS_KEY_ID} AWS_SECRET_ACCESS_KEY=${SERVERLESS_PROD_AWS_SECRET_ACCESS_KEY} AWS_SESSION_TOKEN=${SERVERLESS_PROD_AWS_SESSION_TOKEN} aws ecr get-login-password --region ${AWS_REGION} --output text").trim()
            }
          }
        }
      }
    }


    stage('Maven Artifacts') {
      agent {
        kubernetes {
          inheritFrom 'maven21'
          defaultContainer 'maven'
        }
      }

      steps{
          checkout scm
          sh 'export SONAR_SCANNER_OPTS=-Xmx2048m'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-cloud-utils:${CLOUD_UTILS_NEXUS_VERSION} -s settings.xml'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-workspace-services-thrift:${WRK_THRIFT_NEXUS_VERSION} -s settings.xml'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-services-thrift:${THRIFT_NEXUS_VERSION} -s settings.xml'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-auth-interface:${AUTH_NEXUS_VERSION} -s settings.xml'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-delta-log-reader:${DELTA_LOG_READER_VERSION} -s settings.xml'
          sh 'mvn dependency:get -Dartifact=io.e6x:e6-common:${COMMON_NEXUS_VERSION} -s settings.xml'
          sh 'mvn install -s settings.xml -Drevision=${TAG_VALUE} -Dwrk-thrift-revision=${WRK_THRIFT_NEXUS_VERSION} -Dservices-thrift-revision=${THRIFT_NEXUS_VERSION} -Dauth-interface-revision=${AUTH_NEXUS_VERSION} -Dcloud-utils-revision=${CLOUD_UTILS_NEXUS_VERSION} -De6-delta-reader-revision=${DELTA_LOG_READER_VERSION} -Dcommon-revision=${COMMON_NEXUS_VERSION}'
          sh 'echo \"Nexus Version deployed: ${TAG_VALUE}\"'
          dir ('target') {
            stash includes: '*.jar', name: 'targetJar'
          }
          sh 'wget -O jmx.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROM_VERSION}/jmx_prometheus_javaagent-${JMX_PROM_VERSION}.jar '
          stash includes: 'jmx.jar', name: 'jmxJar'
      }
    }

    stage('Docker Artifacts') {
      agent {
        kubernetes {
          inheritFrom 'dockerarchbuild'
          defaultContainer 'docker'
        }
      }

      environment {
        GCP_SA_PATH = credentials('JENKINS_GCP_SA')
        ACR_TOKEN = credentials('ACR_TOKEN')
      }

      steps {
        checkout scm
        unstash 'targetJar'
        unstash 'jmxJar'
        sh 'apk add git curl'
        sh 'cp e6-transpiler-${TAG_VALUE}-jar-with-dependencies.jar ops/app.jar'
        sh 'cp jmx.jar ops/jmx.jar'
        sh 'docker login -u oauth2accesstoken -p ${GCP_DOCKER_TOKEN} https://us-docker.pkg.dev'
        sh 'docker login --username harshithraj243435 --password ${HUB_TOKEN} docker.io'
        sh "docker buildx create --name mybuilder --driver docker-container --use --platform linux/arm64,linux/amd64 --driver-opt network=host --buildkitd-flags '--allow-insecure-entitlement network.host' --bootstrap"
        
        // Install Docker Scout
        sh 'curl -sSfL https://raw.githubusercontent.com/docker/scout-cli/main/install.sh | sh -s -- -b /usr/local/bin'
        dir ('ops') {
          sh 'docker buildx build --no-cache --platform linux/amd64 -t ${RELEASE_NAME} -t ${PROD_IMAGE}:${TAG_VALUE} --network host --load .'
        }
        // Analyze with Docker Scout and check for critical or high vulnerabilities
        script {
          // Run Docker Scout and capture output
          def scoutResult = sh(
            script: "docker-scout cves ${RELEASE_NAME} --only-fixed --exit-code",
            returnStatus: true
          )
          if (scoutResult == 0) {
            echo "Docker Scout scan passed. No critical or high vulnerabilities found."
            
            // Build and push multi-platform to both GCP and Docker Hub
            dir ('ops') {
              sh 'docker buildx build --no-cache --platform linux/amd64,linux/arm64 --provenance=true --sbom=true -t ${HUB_IMAGE}:${TAG_VALUE} -t ${PROD_IMAGE}:${TAG_VALUE} --network host --push .'
            }
          } else {
            echo "Docker Scout found vulnerabilities"
            error('Docker Scout scan failed - critical or high vulnerabilities found. Image will not be pushed.')
          }
        }
        sh 'apk add skopeo'
        sh 'docker login -u AWS -p ${ECR_TOKEN} 670514002493.dkr.ecr.us-east-1.amazonaws.com'
        sh 'docker login --username harshithraj243435 --password ${HUB_TOKEN} docker.io'
        sh 'skopeo copy --all docker://${HUB_IMAGE}:${TAG_VALUE} docker://${DEV_IMAGE}:${TAG_VALUE}'
        sh 'skopeo copy --all docker://${HUB_IMAGE}:${TAG_VALUE} docker://${DEV_IMAGE}:latest'
        sh 'echo "Production image successfully copied to Dev registry (AWS ECR)"'

        // Push to ACR
        sh 'docker login --username e6labs --password ${ACR_TOKEN} e6labs.azurecr.io'
        sh 'skopeo copy --all docker://${HUB_IMAGE}:${TAG_VALUE} docker://${ACR_IMAGE}:${TAG_VALUE}'
        sh 'echo "Image pushed to ACR"'

        // Push to Serverless Beta ECR
        sh 'docker login -u AWS -p ${SERVERLESS_BETA_ECR_TOKEN} 908027423391.dkr.ecr.us-east-1.amazonaws.com'
        sh 'skopeo copy --all docker://${HUB_IMAGE}:${TAG_VALUE} docker://${SERVERLESS_BETA_IMAGE}:${TAG_VALUE}'
        sh 'echo "Image pushed to Serverless Beta ECR"'

        // Push to Serverless Prod ECR
        sh 'docker login -u AWS -p ${SERVERLESS_PROD_ECR_TOKEN} 390844744777.dkr.ecr.us-east-1.amazonaws.com'
        sh 'skopeo copy --all docker://${HUB_IMAGE}:${TAG_VALUE} docker://${SERVERLESS_PROD_IMAGE}:${TAG_VALUE}'
        sh 'echo "Image pushed to Serverless Prod ECR"'
      }
    }

    // stage('Trigger Downstream builds - PR') {
    //   steps {
    //     build job: "adhoc_pipes/regression_pr/main", wait: true,
    //     parameters: [
    //     string(name: 'STORAGE_NEXUS_VERSION', value: env.TAG_VALUE)
    //     ]
    //   }
    // }

    stage('Git tagging') {
      steps {
        checkout scm

        script {
          env.GITCOMMIT=sh(returnStdout: true, script: "git log -n 1 --pretty=format:'%h'").trim()
        }

        sh 'git config user.email "ci@e6xlabs.cloud"'
        sh 'git config user.name "Jenkins CI"'
        withCredentials([usernamePassword(credentialsId: 'repo_access', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
            sh 'git tag -m \"commit id: ${GITCOMMIT} nexus build: ${TAG_VALUE}\" -a v${TAG_VALUE}'
            sh 'git push https://${GIT_USERNAME}:${GIT_PASSWORD}@${REPOSITORY} v${TAG_VALUE}'
        }
      }
    }

  }
}
