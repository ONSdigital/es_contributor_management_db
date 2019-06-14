variable "username" {type="string"}
variable "password" {type="string"}


provider "aws" {
    region = "eu-west-2"
}

resource "aws_db_instance" "es-results" {
    allocated_storage = 5
    storage_type = "gp2"
    engine = "postgres"
    engine_version = 11.1
    instance_class = "db.t3.small"
    name = "es_results_db"
    username = "${var.username}"
    password = "${var.password}"
    backup_retention_period = 7
    identifier = "es-results-db"
    skip_final_snapshot = false
    publicly_accessible = true

  provisioner "local-exec" {
    command =  "test-data/load_data.sh ${var.username} ${var.password}"
  }
}
