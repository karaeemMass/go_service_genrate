#!/usr/bin/env python3
"""new_service.py - Go + gRPC service scaffolding generator."""

import shutil
import subprocess
import sys
from pathlib import Path

# ------------------------------------------------------------------ templates
# __MODULE__ and __DB__ are replaced with the values you enter.

FILES: dict[str, str] = {}

FILES["cmd/server/config.yaml"] = r"""server:
  port: 50051

database:
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  name: __DB__
  sslmode: disable
"""

FILES["internal/config/config.go"] = r"""package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	SSLMode  string `yaml:"sslmode"`
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.Name, d.SSLMode)
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}
"""

FILES["proto/user.proto"] = r"""syntax = "proto3";

package user.v1;

option go_package = "__MODULE__/proto/pb;pb";

service UserService {
  rpc CreateUser(CreateUserRequest) returns (User);
  rpc GetUser(GetUserRequest) returns (User);
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
  rpc UpdateUser(UpdateUserRequest) returns (User);
  rpc DeleteUser(DeleteUserRequest) returns (DeleteUserResponse);
}

message User {
  int64 id = 1;
  string name = 2;
  string email = 3;
  string created_at = 4;
}

message CreateUserRequest {
  string name = 1;
  string email = 2;
}

message GetUserRequest {
  int64 id = 1;
}

message ListUsersRequest {}

message ListUsersResponse {
  repeated User users = 1;
}

message UpdateUserRequest {
  int64 id = 1;
  string name = 2;
  string email = 3;
}

message DeleteUserRequest {
  int64 id = 1;
}

message DeleteUserResponse {
  bool success = 1;
}
"""

FILES["internal/model/user.go"] = r"""package model

import (
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrInvalidInput = errors.New("invalid input")
)

type User struct {
	ID        int64
	Name      string
	Email     string
	CreatedAt time.Time
}
"""

FILES["internal/repository/user_repo.go"] = r"""package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"__MODULE__/internal/model"
)

type UserRepository interface {
	Create(ctx context.Context, u *model.User) error
	GetByID(ctx context.Context, id int64) (*model.User, error)
	List(ctx context.Context) ([]*model.User, error)
	Update(ctx context.Context, u *model.User) error
	Delete(ctx context.Context, id int64) error
}

type userRepo struct {
	db *pgxpool.Pool
}

func NewUserRepository(db *pgxpool.Pool) UserRepository {
	return &userRepo{db: db}
}

// Migrate creates the tables if they do not exist. Swap it for a migrations tool if you prefer.
func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id         BIGSERIAL PRIMARY KEY,
			name       TEXT NOT NULL,
			email      TEXT NOT NULL UNIQUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

func (r *userRepo) Create(ctx context.Context, u *model.User) error {
	return r.db.QueryRow(ctx,
		`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, created_at`,
		u.Name, u.Email,
	).Scan(&u.ID, &u.CreatedAt)
}

func (r *userRepo) GetByID(ctx context.Context, id int64) (*model.User, error) {
	var u model.User
	err := r.db.QueryRow(ctx,
		`SELECT id, name, email, created_at FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, model.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) List(ctx context.Context) ([]*model.User, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, name, email, created_at FROM users ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []*model.User
	for rows.Next() {
		var u model.User
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt); err != nil {
			return nil, err
		}
		users = append(users, &u)
	}
	return users, rows.Err()
}

func (r *userRepo) Update(ctx context.Context, u *model.User) error {
	err := r.db.QueryRow(ctx,
		`UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING created_at`,
		u.Name, u.Email, u.ID,
	).Scan(&u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrNotFound
	}
	return err
}

func (r *userRepo) Delete(ctx context.Context, id int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}
"""

FILES["internal/service/user_service.go"] = r"""package service

import (
	"context"
	"fmt"
	"strings"

	"__MODULE__/internal/model"
	"__MODULE__/internal/repository"
)

type UserService struct {
	repo repository.UserRepository
}

func NewUserService(repo repository.UserRepository) *UserService {
	return &UserService{repo: repo}
}

func (s *UserService) Create(ctx context.Context, name, email string) (*model.User, error) {
	if err := validate(name, email); err != nil {
		return nil, err
	}
	u := &model.User{Name: name, Email: email}
	if err := s.repo.Create(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}

func (s *UserService) Get(ctx context.Context, id int64) (*model.User, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *UserService) List(ctx context.Context) ([]*model.User, error) {
	return s.repo.List(ctx)
}

func (s *UserService) Update(ctx context.Context, id int64, name, email string) (*model.User, error) {
	if err := validate(name, email); err != nil {
		return nil, err
	}
	u := &model.User{ID: id, Name: name, Email: email}
	if err := s.repo.Update(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}

func (s *UserService) Delete(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

func validate(name, email string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: name is required", model.ErrInvalidInput)
	}
	if !strings.Contains(email, "@") {
		return fmt.Errorf("%w: invalid email", model.ErrInvalidInput)
	}
	return nil
}
"""

FILES["internal/handler/user_handler.go"] = r"""package handler

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"__MODULE__/internal/model"
	"__MODULE__/internal/service"
	pb "__MODULE__/proto/pb"
)

type UserHandler struct {
	pb.UnimplementedUserServiceServer
	svc *service.UserService
}

func NewUserHandler(svc *service.UserService) *UserHandler {
	return &UserHandler{svc: svc}
}

func (h *UserHandler) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	u, err := h.svc.Create(ctx, req.GetName(), req.GetEmail())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProto(u), nil
}

func (h *UserHandler) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	u, err := h.svc.Get(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProto(u), nil
}

func (h *UserHandler) ListUsers(ctx context.Context, _ *pb.ListUsersRequest) (*pb.ListUsersResponse, error) {
	users, err := h.svc.List(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}
	resp := &pb.ListUsersResponse{}
	for _, u := range users {
		resp.Users = append(resp.Users, toProto(u))
	}
	return resp, nil
}

func (h *UserHandler) UpdateUser(ctx context.Context, req *pb.UpdateUserRequest) (*pb.User, error) {
	u, err := h.svc.Update(ctx, req.GetId(), req.GetName(), req.GetEmail())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProto(u), nil
}

func (h *UserHandler) DeleteUser(ctx context.Context, req *pb.DeleteUserRequest) (*pb.DeleteUserResponse, error) {
	if err := h.svc.Delete(ctx, req.GetId()); err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.DeleteUserResponse{Success: true}, nil
}

func toProto(u *model.User) *pb.User {
	return &pb.User{
		Id:        u.ID,
		Name:      u.Name,
		Email:     u.Email,
		CreatedAt: u.CreatedAt.Format(time.RFC3339),
	}
}

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, model.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, model.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
"""

FILES["cmd/server/main.go"] = r"""package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"__MODULE__/internal/config"
	"__MODULE__/internal/handler"
	"__MODULE__/internal/repository"
	"__MODULE__/internal/service"
	pb "__MODULE__/proto/pb"
)

func main() {
	cfgPath := flag.String("config", "cmd/server/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.Database.DSN())
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	if err := repository.Migrate(ctx, db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// wiring: repo -> service -> handler
	userRepo := repository.NewUserRepository(db)
	userSvc := service.NewUserService(userRepo)
	userHandler := handler.NewUserHandler(userSvc)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.Port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterUserServiceServer(srv, userHandler)
	reflection.Register(srv) // lets you test with grpcurl

	go func() {
		<-ctx.Done()
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("gRPC server listening on :%d", cfg.Server.Port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
"""

FILES["README.md"] = r"""# __MODULE__

gRPC service written in Go. Database: PostgreSQL (`__DB__`).

## Running

```bash
# 1) generate protobuf code
protoc --go_out=. --go_opt=module=__MODULE__ \
       --go-grpc_out=. --go-grpc_opt=module=__MODULE__ \
       proto/user.proto

# 2) run
go run ./cmd/server
```

All settings live in `cmd/server/config.yaml`.

## Structure

- `cmd/server`         : main + config.yaml
- `internal/config`    : config loading
- `internal/handler`   : gRPC handlers
- `internal/service`   : business logic
- `internal/repository`: database layer
- `internal/model`     : structs
- `proto`              : protobuf definitions
"""


# ------------------------------------------------------------------ helpers
def ask(prompt: str) -> str:
    value = input(prompt).strip()
    if not value:
        sys.exit("A value is required.")
    return value


def run(cmd: list[str], cwd: Path) -> bool:
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def main() -> None:
    module = ask("Project name (module name, e.g. github.com/me/orders): ")
    db_name = ask("Database name: ")

    root = Path(module.rstrip("/").split("/")[-1])
    if root.exists():
        sys.exit(f"Directory {root} already exists.")

    # Create the files
    for rel_path, content in FILES.items():
        content = content.replace("__MODULE__", module).replace("__DB__", db_name)
        path = root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"  created {path}")

    # go mod init
    if shutil.which("go") is None:
        print("⚠ Go is not installed. Run go mod init and go mod tidy yourself.")
        return
    run(["go", "mod", "init", module], root)

    # protoc
    tools = ["protoc", "protoc-gen-go", "protoc-gen-go-grpc"]
    protoc_cmd = [
        "protoc",
        "--go_out=.", f"--go_opt=module={module}",
        "--go-grpc_out=.", f"--go-grpc_opt=module={module}",
        "proto/user.proto",
    ]
    if all(shutil.which(t) for t in tools):
        if run(protoc_cmd, root):
            print("✔ protobuf code generated")
        else:
            print("⚠ protoc failed, check the errors above.")
    else:
        print("⚠ protoc or its plugins are not installed. Install them, then run:")
        print("  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest")
        print("  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest")
        print("  " + " ".join(protoc_cmd))

    if not run(["go", "mod", "tidy"], root):
        print("⚠ go mod tidy failed (probably because the protobuf code has not been generated yet). Run it after generating.")

    print(f"\n✔ Project created at: {root.resolve()}")
    print("  Run it with: go run ./cmd/server")


if __name__ == "__main__":
    main()
