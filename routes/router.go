package routes

import (
	"log"
	"streamingservice/controllers"
	"streamingservice/db"
	"streamingservice/models"
	"time"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
)

var identityKey = "id"
var DB = db.New()

type login struct {
	Username string `form:"username" json:"username" binding:"required"`
	Password string `form:"password" json:"password" binding:"required"`
}

func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "*")

		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func New() *gin.Engine {

	router := gin.Default()
	// router.Use(cors.Default())

	authMiddleware, err := jwt.New(&jwt.GinJWTMiddleware{
		Realm:       "test zone",
		Key:         []byte("secret key"),
		Timeout:     time.Hour * 480,
		MaxRefresh:  time.Hour,
		IdentityKey: identityKey,
		PayloadFunc: func(data interface{}) jwt.MapClaims {
			if v, ok := data.(*models.UserEntity); ok {
				return jwt.MapClaims{
					identityKey: v.Username,
				}
			}
			return jwt.MapClaims{}
		},
		IdentityHandler: func(c *gin.Context) interface{} {
			claims := jwt.ExtractClaims(c)
			return &models.UserEntity{
				Username: claims[identityKey].(string),
			}
		},
		Authenticator: func(c *gin.Context) (interface{}, error) {
			var loginVals login
			if err := c.ShouldBind(&loginVals); err != nil {
				return "", jwt.ErrMissingLoginValues
			}
			password := loginVals.Password
			var user models.UserEntity
			if loginVals.Username != "" {

				if err := DB.Where("username=?", loginVals.Username).First(&user).Error; err != nil {
					return "", jwt.ErrFailedAuthentication
				}
				if !user.CheckPassword(password) {
					return "", jwt.ErrFailedAuthentication
				}
				return &user, nil
			}

			return nil, jwt.ErrFailedAuthentication
		},
		Authorizator: func(data interface{}, c *gin.Context) bool {
			if v, ok := data.(*models.UserEntity); ok && v.Username == "admin" {
				return true
			}

			return false
		},
		Unauthorized: func(c *gin.Context, code int, message string) {
			c.JSON(code, gin.H{
				"code":    code,
				"message": message,
			})
		},
		// TokenLookup is a string in the form of "<source>:<name>" that is used
		// to extract token from the request.
		// Optional. Default value "header:Authorization".
		// Possible values:
		// - "header:<name>"
		// - "query:<name>"
		// - "cookie:<name>"
		// - "param:<name>"
		TokenLookup: "header: Authorization, query: token, cookie: jwt",
		// TokenLookup: "query:token",
		// TokenLookup: "cookie:token",

		// TokenHeadName is a string in the header. Default value is "Bearer"
		TokenHeadName: "Bearer",

		// TimeFunc provides the current time. You can override it to use another time value. This is useful for testing or if your server uses a different time zone than your tokens.
		TimeFunc: time.Now,
	})

	if err != nil {
		log.Fatal("JWT Error:" + err.Error())
	}

	// When you use jwt.New(), the function is already automatically called for checking,
	// which means you don't need to call it again.
	errInit := authMiddleware.MiddlewareInit()

	if errInit != nil {
		log.Fatal("authMiddleware.MiddlewareInit() Error:" + errInit.Error())
	}

	router.Use(CORS())
	router.POST("/login", authMiddleware.LoginHandler)

	router.GET("/connectors", controllers.FindConnectors)
	router.POST("/connectors", controllers.CreateConnector)
	router.GET("/connector-classes", controllers.GetConnectorClasses)
	router.GET("/convertors", controllers.GetConvertors)
	router.POST("/connectors/start/:entityId", controllers.PostConnector)
	router.GET("/connectors/:entityId", controllers.FindConnector)
	router.PUT("/connectors/:entityId", controllers.EditConnector)
	router.DELETE("/connectors/:entityId", controllers.RemoveConnector)
	router.POST("/connectors/stop/:entityName", controllers.StopConnector)
	router.POST("/connectors-plugins/upload", controllers.UploadConnectorPlugin)
	router.PUT("/connectors-validate", controllers.ValidateConnector)
	router.GET("/workers", controllers.FindWorkers)
	router.POST("/workers/:entityId/start", controllers.StartKafkaConnect)
	router.POST("/workers/:entityId/stop", controllers.StopKafkaConnect)
	router.GET("/worker/:entityId", controllers.FindWorker)
	router.POST("/user/create", controllers.CreateUser)
	return router
}
