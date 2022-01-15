terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }
}

variable "your_region" {
  type        = string
  description = "Which AWS Region. The options are here https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions."
}

variable "your_public_key" {
  type        = string
  description = "This will be in ~/.ssh/id_rsa.pub by default."
}

provider "aws" {
  profile = "default"
  region  = var.your_region
}

resource "aws_security_group" "ragedb" {
  ingress {
    description = "Receive SSH from everywhere."
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "Receive RageDB 7243 from everywhere."
    from_port   = 7243
    to_port     = 7243
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    description = "Send everywhere."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "RageDB"
  }
}

resource "aws_key_pair" "home" {
  key_name   = "Home"
  public_key = var.your_public_key
}

resource "aws_instance" "ragedb" {
  ami                         = "ami-0883f2d26628ad0cf"
  instance_type               = "r5.2xlarge"
  vpc_security_group_ids      = [aws_security_group.ragedb.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.home.key_name
  user_data                   = <<-EOF
    #!/bin/bash
    sudo dnf update -y
    sudo dnf install -y  git-all luajit perl pip screen
    git clone https://github.com/scylladb/seastar.git
    cd seastar
    sudo ./install-dependencies.sh
    ./configure.py --mode=release --prefix=/usr/local
    sudo ninja -C build/release install
    cd ..
    pip install --user conan
    sudo ln -s ~/.local/bin/conan /usr/bin/conan
    git clone https://github.com/ragedb/ragedb.git
    cd ragedb
    mkdir build
    cd build
    cmake .. -DCMAKE_BUILD_TYPE=Release
    cmake --build . --target ragedb
    cd bin
    sudo ./ragedb
    EOF
  tags = {
    Name = "RageDB"
  }
}

output "instance_ip_addr" {
  value = aws_instance.ragedb.public_ip
}
