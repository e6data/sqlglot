pipeline {
    environment {
        RELEASE_NAME = "transpiler"
        IMAGE_TAG_PREFIX = "3.0.${BUILD_NUMBER}-"
        
        GCP_PROJECT_ID = credentials('GCP_PROJECT_ID')
        PROD_IMAGE = "us-docker.pkg.dev/${GCP_PROJECT_ID}/e6-engine/${RELEASE_NAME}"

        TRIVY_VERSION = "v0.69.2"
        TRIVY_OPTIONS = "--db-repository public.ecr.aws/aquasecurity/trivy-db"

        ACR_TOKEN = credentials('ACR_TOKEN')
        AZURE_IMAGE = "e6labs.azurecr.io/${RELEASE_NAME}"
        
        AWS_REGION = "us-east-1"
        ASSUMED_ROLE_ARN = "arn:aws:iam::670514002493:role/cross-account-jenkins-access"
        DEV_IMAGE = "670514002493.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}:latest"

        // Serverless beta/prod push targets (added)
        SERVERLESS_BETA_ROLE_ARN = "arn:aws:iam::908027423391:role/cross-account-jenkins-access"
        SERVERLESS_PROD_ROLE_ARN = "arn:aws:iam::390844744777:role/cross-account-jenkins-access"
        SERVERLESS_BETA_IMAGE = "908027423391.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"
        SERVERLESS_PROD_IMAGE = "390844744777.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"
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
        // stage('Sonarqube Scan') {
        //     agent {
        //         kubernetes {
        //             inheritFrom 'docker'
        //             defaultContainer 'docker'
        //         }
        //     }

        //     environment {
        //         SCANNER_HOME = tool 'sonarqube'
        //     }

        //     steps {
        //         withSonarQubeEnv('sonarqube-jenkins') {
        //             checkout scm
        //             sh '${SCANNER_HOME}/bin/sonar-scanner'
        //         }
        //     }
        // }

        // stage("Quality Gate") {
        //     steps {
        //         timeout(time: 1, unit: 'HOURS') {
        //             waitForQualityGate abortPipeline: true
        //         }
        //     }
        // }

        // stage('Trivy Scan') {
        //     agent {
        //         kubernetes {
        //             inheritFrom 'pythondarm'
        //             defaultContainer 'python'
        //         }
        //     }

        //     steps {
        //         checkout scm
        //         sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
        //         sh 'mkdir -p /tmp/trivy/'
        //         sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
        //         sh 'tar -xvf /tmp/trivy/db.tar.gz'
        //         script {
        //             def trivyResult = sh(
        //                 script: "trivy fs --exit-code 1 --cache-dir='/tmp/trivy' ${TRIVY_OPTIONS} --no-progress --scanners vuln,misconfig,secret .",
        //                 returnStatus: true
        //             )
        //             if (trivyResult == 0) {
        //                 // Trivy scan passed, push the image
        //                 sh 'echo "Trivy scan passed for Python Scripts."'
        //             }
        //             else {
        //                 error('Trivy scan failed')
        //             }
        //         }
        //     }
        // }

        stage('Cloud Authentication') {
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
                    env.GIT_COMMIT_HASH = sh(script: 'git rev-parse --short HEAD', returnStdout: true)
                    env.TAG_VALUE = "${IMAGE_TAG_PREFIX}${GIT_COMMIT_HASH}"
                    env.GCP_DOCKER_TOKEN = sh(returnStdout: true, script: "gcloud auth print-access-token").trim() 

                    // Serverless beta/prod ECR tokens (added) — assumed by the OIDC role BEFORE
                    // the dev assume below overwrites AWS_ACCESS_KEY_ID/SECRET/SESSION.
                    env.SERVERLESS_BETA_TEMP_ROLE = sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${SERVERLESS_BETA_ROLE_ARN} --role-session-name transpiler-slbeta-${BUILD_NUMBER}').trim()
                    env.SERVERLESS_BETA_ECR_TOKEN = sh(returnStdout: true, script: 'AWS_ACCESS_KEY_ID=$(echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.AccessKeyId") AWS_SECRET_ACCESS_KEY=$(echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.SecretAccessKey") AWS_SESSION_TOKEN=$(echo $SERVERLESS_BETA_TEMP_ROLE | jq -r ".Credentials.SessionToken") aws ecr get-login-password --region ${AWS_REGION} --output text').trim()

                    env.SERVERLESS_PROD_TEMP_ROLE = sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${SERVERLESS_PROD_ROLE_ARN} --role-session-name transpiler-slprod-${BUILD_NUMBER}').trim()
                    env.SERVERLESS_PROD_ECR_TOKEN = sh(returnStdout: true, script: 'AWS_ACCESS_KEY_ID=$(echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.AccessKeyId") AWS_SECRET_ACCESS_KEY=$(echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.SecretAccessKey") AWS_SESSION_TOKEN=$(echo $SERVERLESS_PROD_TEMP_ROLE | jq -r ".Credentials.SessionToken") aws ecr get-login-password --region ${AWS_REGION} --output text').trim()

                    env.TEMP_ROLE = sh(returnStdout: true, script: 'aws sts assume-role --role-arn ${ASSUMED_ROLE_ARN} --role-session-name storage-service-${BUILD_NUMBER}').trim()
                    env.AWS_ACCESS_KEY_ID = sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.AccessKeyId"').trim()
                    env.AWS_SECRET_ACCESS_KEY = sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.SecretAccessKey"').trim()
                    env.AWS_SESSION_TOKEN = sh(returnStdout: true, script: 'echo $TEMP_ROLE | jq -r ".Credentials.SessionToken"').trim()
                    env.ECR_TOKEN = sh(returnStdout: true, script: "aws ecr get-login-password --region ${AWS_REGION} --output text").trim()
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
                        // sh 'mkdir -p /tmp/trivy/'
                        // sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
                        // sh 'tar -xvf /tmp/trivy/db.tar.gz'
                        sh 'docker buildx build --no-cache --platform linux/arm64  -t ${RELEASE_NAME} --load .'
                        sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
                        script {
                                // Trivy scan passed, push the image
                                sh 'docker buildx build --platform linux/arm64 -t ${PROD_IMAGE} --push --output=type=image,push-by-digest=true --metadata-file meta-arm64.json .'
                                env.ARM_HASH = sh(returnStdout: true, script: "cat meta-arm64.json | jq -r '.\"containerimage.digest\"'").trim()
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
                        // sh 'mkdir -p /tmp/trivy/'
                        // sh 'aws s3 cp s3://e6-trivy-db/db.tar.gz /tmp/trivy/'
                        // sh 'tar -xvf /tmp/trivy/db.tar.gz'
                        sh 'docker buildx build --no-cache --platform linux/amd64  -t ${RELEASE_NAME} --load .'
                        sh 'curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin ${TRIVY_VERSION}'
                        script {
                                // Trivy scan passed, push the image
                                sh 'docker buildx build --platform linux/amd64 -t ${PROD_IMAGE} --push --output=type=image,push-by-digest=true --metadata-file meta-amd64.json .'
                                env.AMD_HASH = sh(returnStdout: true, script: "cat meta-amd64.json | jq -r '.\"containerimage.digest\"'").trim()
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
                sh 'apk add skopeo'
                sh 'skopeo login -u oauth2accesstoken -p ${GCP_DOCKER_TOKEN} https://us-docker.pkg.dev'
                sh 'skopeo login --username AWS --password ${ECR_TOKEN} 670514002493.dkr.ecr.us-east-1.amazonaws.com'
                sh 'skopeo login --username e6labs --password ${ACR_TOKEN} e6labs.azurecr.io'
                sh 'skopeo copy docker://${PROD_IMAGE}:${TAG_VALUE} docker://${DEV_IMAGE}'
                sh 'skopeo copy docker://${PROD_IMAGE}:${TAG_VALUE} docker://670514002493.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}:${TAG_VALUE}'
                sh 'skopeo copy docker://${PROD_IMAGE}:${TAG_VALUE} docker://${AZURE_IMAGE}:${TAG_VALUE}'

                // Push to Serverless Beta ECR (added)
                sh 'skopeo login --username AWS --password ${SERVERLESS_BETA_ECR_TOKEN} 908027423391.dkr.ecr.us-east-1.amazonaws.com'
                sh 'skopeo copy docker://${PROD_IMAGE}:${TAG_VALUE} docker://${SERVERLESS_BETA_IMAGE}:${TAG_VALUE}'

                // Push to Serverless Prod ECR (added)
                sh 'skopeo login --username AWS --password ${SERVERLESS_PROD_ECR_TOKEN} 390844744777.dkr.ecr.us-east-1.amazonaws.com'
                sh 'skopeo copy docker://${PROD_IMAGE}:${TAG_VALUE} docker://${SERVERLESS_PROD_IMAGE}:${TAG_VALUE}'
            }
        }
    }
}
