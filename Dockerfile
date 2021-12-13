FROM golang:alpine

WORKDIR /streamservice

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o main .

EXPOSE 8000

COPY wait-for.sh ./

# Run the created binary executable after wait for mysql container to be up
CMD ["./wait-for.sh" , "mysql:3306" , "--timeout=300" , "--" , "./main"]
#CMD [ "go", "run", "main.go" ]
