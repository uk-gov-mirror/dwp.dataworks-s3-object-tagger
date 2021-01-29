resource "aws_ecr_repository" "dataworks-repo-template-docker" {
  name = "dataworks-repo-template-docker"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/dataworks-repo-template-docker" }
  )
}

resource "aws_ecr_repository_policy" "dataworks-repo-template-docker" {
  repository = aws_ecr_repository.dataworks-repo-template-docker.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.dataworks-repo-template-docker.repository_url
}
