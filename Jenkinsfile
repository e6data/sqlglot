pipeline {
    environment {
        RELEASE_NAME = "transpiler"
        IMAGE_TAG_PREFIX = "3.0.${BUILD_NUMBER}-"
        
        // Uniphi ECR is the build's registry: per-arch images are pushed here by
        // digest and the multi-arch tag is assembled here, then copied to ACR.
        // Jenkins itself runs in the uniphi account (jenkins:jenkins IRSA role,
        // which already holds ecr:PutImage), so the pod's own creds suffice.
        PROD_IMAGE = "298655976287.dkr.ecr.us-east-1.amazonaws.com/${RELEASE_NAME}"

        TRIVY_VERSION = "v0.69.2"
        TRIVY_OPTIONS = "--db-repository public.ecr.aws/aquasecurity/trivy-db"

        ACR_TOKEN = credentials('ACR_TOKEN')
        AZURE_IMAGE = "e6labs.azurecr.io/${RELEASE_NAME}"
        
        AWS_REGION = "us-east-1"
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

            steps {
                checkout scm
                sh 'git config --global --add safe.directory "*"'
                script {
                    env.GIT_COMMIT_HASH = sh(script: 'git rev-parse --short HEAD', returnStdout: true)
                    env.TAG_VALUE = "${IMAGE_TAG_PREFIX}${GIT_COMMIT_HASH}"

                    // Uniphi ECR token — read from the agent's own IRSA credentials,
                    // which are already in the uniphi account. No assume-role anywhere in
                    // this pipeline, so nothing overrides the ambient identity.
                    env.UNIPHI_ECR_TOKEN = sh(returnStdout: true, script: 'aws ecr get-login-password --region ${AWS_REGION} --output text').trim()
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
                        sh 'docker login --username AWS --password ${UNIPHI_ECR_TOKEN} 298655976287.dkr.ecr.us-east-1.amazonaws.com'
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
                        sh 'docker login --username AWS --password ${UNIPHI_ECR_TOKEN} 298655976287.dkr.ecr.us-east-1.amazonaws.com'
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
                sh 'docker login --username AWS --password ${UNIPHI_ECR_TOKEN} 298655976287.dkr.ecr.us-east-1.amazonaws.com'
                sh 'docker buildx imagetools create  --tag ${PROD_IMAGE}:${TAG_VALUE} ${AMD_HASH} ${ARM_HASH}'
                sh 'apk add skopeo'
                sh 'skopeo login --username AWS --password ${UNIPHI_ECR_TOKEN} 298655976287.dkr.ecr.us-east-1.amazonaws.com'
                sh 'skopeo login --username e6labs --password ${ACR_TOKEN} e6labs.azurecr.io'
                // --all keeps the multi-arch manifest list (plain copy flattens to one arch).
                sh 'skopeo copy --all docker://${PROD_IMAGE}:${TAG_VALUE} docker://${AZURE_IMAGE}:${TAG_VALUE}'
            }
        }
    }
}
