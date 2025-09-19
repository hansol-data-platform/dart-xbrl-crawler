
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 818263291911.dkr.ecr.ap-northeast-2.amazonaws.com


docker build --platform linux/amd64 -t dart-xbrl-crawler .


docker tag dart-xbrl-crawler:latest 818263291911.dkr.ecr.ap-northeast-2.amazonaws.com/youngjunlee/dart-xbrl-crawler:latest


docker push 818263291911.dkr.ecr.ap-northeast-2.amazonaws.com/youngjunlee/dart-xbrl-crawler:latest