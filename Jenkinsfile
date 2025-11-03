pipeline { 
    agent any

    environment {
        TARGET_SERVER = '172.16.0.103'
        TARGET_USER = 'root'
        TARGET_PASSWORD = 'Teamw0rk@1'
        DEPLOY_PATH = 'Horizan_reports'
    }

    stages {
         stage('Checkout') {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: '*/poojitha']],  // ✅ Correct syntax
                    userRemoteConfigs: [[
                        url: 'https://github.com/thinkcloud-in/DaaS_backend_2025_with_temporal.git',
                        credentialsId: 'rcv-git'
                    ]]
                ])
            }
        }
        // stage('Git Checkout') {
        //     steps {
        //         git credentialsId: 'rcv-git', url: 'https://github.com/thinkcloud-in/DaaS_backend_2025_with_temporal.git'  ,branch: 'main' 
        //     }
        // }

        stage('Docker Build & Tag') {
            steps {
                script {
                    withDockerRegistry([credentialsId: 'dokcer-id']) { // Use your Docker Hub credentials ID
                        sh 'docker build -t arpits2931/rcvdaasbackend:latest -f Dockerfile .'
                    }
                }
            }
        }

        stage('Docker Push') {
            steps {
                script {
                    withDockerRegistry([credentialsId: 'dokcer-id']) { // Use the same credentials ID
                        sh 'docker push arpits2931/rcvdaasbackend:latest'
                    }
                }
            }
        }

        stage('Run pwd Command') {
            steps {
               script {
                    sh """
                        sshpass -p '${TARGET_PASSWORD}' ssh -T -o StrictHostKeyChecking=no ${TARGET_USER}@${TARGET_SERVER} '
                        docker ps -a --filter "ancestor=arpits2931/rcvdaasbackend:latest" --format "{{.ID}}" | xargs -r docker stop;
                        docker ps -a --filter "ancestor=arpits2931/rcvdaasbackend:latest" --format "{{.ID}}" | xargs -r docker rm;
                        docker images "arpits2931/rcvdaasbackend:latest" --format "{{.ID}}" | xargs -r docker rmi -f;
                        '
                    """
                }
            }
        }

        stage('Change Directory and Run Docker') {
            steps {
                script {
                    sh "sshpass -p '${TARGET_PASSWORD}' ssh -T -o StrictHostKeyChecking=no ${TARGET_USER}@${TARGET_SERVER} 'cd ${DEPLOY_PATH} && docker-compose up -d'"
                }
            }
        }

        stage('Final Stage') {
            steps {
                echo 'All stages have been completed successfully.'
            }
        }
    }
}
