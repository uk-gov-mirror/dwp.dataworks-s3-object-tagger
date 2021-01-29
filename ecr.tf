resource "aws_ecr_repository" "dataworks-s3-object-tagger" {
  name = "dataworks-s3-object-tagger"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/dataworks-s3-object-tagger" }
  )
}

resource "aws_ecr_repository_policy" "dataworks-s3-object-tagger" {
  repository = aws_ecr_repository.dataworks-s3-object-tagger.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.dataworks-s3-object-tagger.repository_url
}
