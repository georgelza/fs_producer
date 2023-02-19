/*
*
*	File		: producer.go
*
* 	Created		: 27 Aug 2021
*
*	Description	:
*
*	Modified	: 27 Aug 2021	- Start
*				: 24 Jan 2023   - Mod for applab sandbox
*
*	By			: George Leonard (georgelza@gmail.com)
*
*
 */

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"

	"github.com/TylerBrock/colorjson"
	log "github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	glog "google.golang.org/grpc/grpclog"
)

type TPgeneral struct {
	hostname   string
	debuglevel int
	loglevel   string
	logformat  string
	testsize   int
	sleep      int
}

type TPkafka struct {
	bootstrapservers  string
	topicname         string
	numpartitions     int
	replicationfactor int
	retension         string
	parseduration     string
	security_protocol string
	sasl_mechanisms   string
	sasl_username     string
	sasl_password     string
	flush_interval    int
}

type TPaddress struct {
	Streetname string  `json:streetname`
	City       string  `json:city`
	State      string  `json:state`
	Zip        string  `json:zip`
	Country    string  `json:country`
	Latitude   float64 `json:latitude`
	Longitude  float64 `json:longitude`
}

type TPccard struct {
	Type   string `json:"type"`
	Number int    `json:"number"`
	Exp    string `json:"exp"`
	Cvv    string `json:"cvv"`
}

type TPjobinfo struct {
	Company    string `json:"company"`
	Title      string `json:"title"`
	Descriptor string `json:"descriptor"`
	Level      string `json:"level"`
}

type TPcontact struct {
	Email string `json:"email"`
	Phone string `json:"phone"`
}

// My fake data structures
type TPperson struct {
	Ssn          string    `json:"ssn"`
	Firstname    string    `json:"firstname"`
	Lastname     string    `json:"lastname"`
	Gender       string    `json:"gender"`
	Address      TPaddress `json:"address"`
	Contact      TPcontact `json:"contact"`
	Ccard        TPccard   `json:"ccard"`
	Job          TPjobinfo `json:"job"`
	Created_date string    `json:"created_date"`
}

type TPamount struct {
	BaseCurrency string `json:"basecurrency"`
	BaseValue    string `json:"basevalue"`
	Burrency     string `json:"currency"`
	Value        string `json:"value"`
}

type TPtags struct {
	Tag    string   `json:"tag"`
	Values []string `json:"values"`
}

type TPmodelData struct {
	Adata string `json:"adata"`
}

type TPmodels struct {
	ModelId    string      `json:"modelid"`
	Score      float64     `json:"score"`
	Confidence float64     `json:"confidence"`
	Tags       []TPtags    `json:"tags"`
	ModelData  TPmodelData `json:"modeldata"`
}

type TPrules struct {
	RuleId string  `json:rul"e"id`
	Score  float64 `json:"score"`
}

type TPscore struct {
	Models []TPmodels `json:"models"`
	Tags   []TPtags   `json:"tags"`
	Rules  []TPrules  `json:"rules"`
}

type TPaggregator struct {
	AggregatorId   string   `json:"aggregatorid"`
	Scores         TPscore  `json:"scores"`
	AggregateScore string   `json:"aggregatescore"`
	MatchedBound   string   `json:"matchedbound"`
	OutputTags     []TPtags `json:"outputtags,omitempty"`
	SuppressedTags []TPtags `json:"suppressedtags,omitempty"`
	Alert          string   `json:"alert"`
	SuppressAlert  string   `json:"suppressalert"`
}

type TPconfigGroups struct {
	Type           string         `json:"type"`
	Version        string         `json:"version"`
	Id             string         `json:"id"`
	TriggeredRules []string       `json:"triggeredRules,omitempty"`
	Aggregators    []TPaggregator `json:"aggregators, omitempty"`
}

type TPversions struct {
	ModelGraph   int              `json:"modelgraph"`
	ConfigGroups []TPconfigGroups `json:"configgroups"`
}

type TPoverallScore struct {
	AggregationModel string  `json:"aggregationModel"`
	OverallScore     float64 `json:"overallscore"`
}

type TPentity struct {
	TenantId     string           `json:"tenantid"`
	EntityType   string           `json:"entitytype"`
	EntityId     string           `json:"entityid"`
	OverallScore TPoverallScore   `json:"overallscore"`
	Models       []TPmodels       `json:"models"`
	FailedModels []string         `json:"failedmodels"`
	OutputTags   []TPtags         `json:"tags"`
	RiskStatus   string           `json:"riskstatus"`
	ConfigGroups []TPconfigGroups `json:"configgroups"`
}

type TPengineResponse struct {
	Entities         []TPentity             `json:"entities"`
	JsonVersion      int                    `json:"jsonversion"`
	OriginatingEvent map[string]interface{} `json:"originatingevent"`
	OutputTime       string                 `json:"outputtime"`
	ProcessorId      string                 `json:"processorid"`
	Versions         TPversions             `json:"versions"`
}

var (
	grpcLog  glog.LoggerV2
	vGeneral TPgeneral
	m        = make(map[string]string)
)

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	// Playing around with Logrus.
	logLevel, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.SetOutput(os.Stdout)
	logFormat := os.Getenv("LOG_FORMAT")
	if logFormat == "JSON" { // if we want logs in JSON for consumption into ELK or SPLUNK, or any other NoSQL DB Store...
		log.SetFormatter(&log.JSONFormatter{})

	}

	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : producer.go")
	fmt.Println("#")
	fmt.Println("#	 Comment   : Fake Kafka Producer - Applab Example")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", time.Now().Format("27-01-2023 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")
	fmt.Println("*")

}

func getBanks() map[int]string {

	ar := make(map[int]string)

	ar[1] = "Standard Bank"
	ar[2] = "ABSA"
	ar[3] = "Nedbank"
	ar[4] = "Capitect Bank Limited"
	ar[5] = "Bank Zero"
	ar[6] = "Thyme Bank"
	ar[7] = "FirstRand Limited"
	ar[8] = "Discovery Bank Limited"
	ar[9] = "African Bank Limited"
	ar[10] = "Bidvest"
	ar[11] = "Grindrod"
	ar[12] = "Investec"
	ar[13] = "Ithala"
	ar[14] = "Sasfin Bank Limited"
	ar[15] = "Ubank"

	return ar
}

func getTransType() map[int]string {

	ar := make(map[int]string)

	ar[1] = "EFT"
	ar[2] = "AC Collection"
	ar[3] = "Payshap"
	ar[4] = "RTC"

	return ar
}

func getDirection() map[int]string {

	ar := make(map[int]string)

	ar[1] = "Inbound"
	ar[2] = "Outbound"

	return ar
}

func CreateTopic(props TPkafka) {

	cm := kafka.ConfigMap{
		"bootstrap.servers":       props.bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
	}
	grpcLog.Infoln("Basic Client ConfigMap compiled")

	if props.sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = props.sasl_mechanisms
		cm["security.protocol"] = props.security_protocol
		cm["sasl.username"] = props.sasl_username
		cm["sasl.password"] = props.sasl_password
		grpcLog.Infoln("Security Authentifaction configured in ConfigMap")

	}

	grpcLog.Infoln("Create Topic Config map compiled")

	adminClient, err := kafka.NewAdminClient(&cm)
	if err != nil {
		grpcLog.Errorln("Failed to create Admin client: %s", err)
		os.Exit(1)
	}
	grpcLog.Infoln("Succeeded to create Admin client")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDuration, err := time.ParseDuration(props.parseduration)
	if err != nil {
		grpcLog.Errorln(fmt.Sprintf("Error Configuring maxDuration via ParseDuration: %s", props.parseduration))
		os.Exit(1)

	}
	grpcLog.Infoln("Configured maxDuration via ParseDuration")

	results, err := adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             props.topicname,
			NumPartitions:     props.numpartitions,
			ReplicationFactor: props.replicationfactor}},
		kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		fmt.Printf("Problem during the topic creation: %v\n", err)
		os.Exit(1)
	}

	// Check for specific topic errors
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError &&
			result.Error.Code() != kafka.ErrTopicAlreadyExists {
			grpcLog.Info(fmt.Sprintf("Topic creation failed for %s: %v", result.Topic, result.Error.String()))
			os.Exit(1)

		} else {
			grpcLog.Info(fmt.Sprintf("Topic creation Succeeded for %s", result.Topic))

		}
	}

	adminClient.Close()

}

func loadGeneralProps() TPgeneral {

	//	var vGeneral TPgeneral

	// Lets identify ourself - helpful concept in a container environment.
	var err interface{}

	vHostname, err := os.Hostname()
	if err != nil {
		grpcLog.Error("Can't retrieve hostname", err)
		//logCtx.Error(fmt.Sprintf("Can't retrieve hostname %s", err))
	}
	vGeneral.hostname = vHostname

	vGeneral.logformat = os.Getenv("LOG_FORMAT")
	vGeneral.loglevel = os.Getenv("LOG_LEVEL")

	// Lets manage how much we print to the screen
	vGeneral.debuglevel, err = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if err != nil {
		grpcLog.Error("1 String to Int convert error: %s", err)

	}

	// Lets manage how much we print to the screen
	vGeneral.testsize, err = strconv.Atoi(os.Getenv("TESTSIZE"))
	if err != nil {
		grpcLog.Error("2 String to Int convert error: %s", err)

	}

	// Lets manage how much we print to the screen
	vGeneral.sleep, err = strconv.Atoi(os.Getenv("SLEEP"))
	if err != nil {
		grpcLog.Error("2 String to Int convert error: %s", err)

	}

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("Hostname is\t\t\t", vGeneral.hostname)
	grpcLog.Info("Debug Level is\t\t", vGeneral.debuglevel)
	grpcLog.Info("Log Level is\t\t\t", vGeneral.loglevel)
	grpcLog.Info("Log Format is\t\t\t", vGeneral.logformat)
	grpcLog.Info("Sleep Duration is\t\t\t", vGeneral.sleep)

	return vGeneral
}

func loadKafkaProps() TPkafka {

	var vKafka TPkafka
	var err interface{}

	// Broker Configuration
	vKafka.bootstrapservers = fmt.Sprintf("%s:%s", os.Getenv("kafka_bootstrap_servers"), os.Getenv("kafka_bootstrap_port"))
	vKafka.topicname = os.Getenv("kafka_topic_name")
	vKafka_NumPartitions := os.Getenv("kafka_num_partitions")
	vKafka_ReplicationFactor := os.Getenv("kafka_replication_factor")
	vKafka.retension = os.Getenv("kafka_retension")
	vKafka.parseduration = os.Getenv("kafka_parseduration")

	vKafka.security_protocol = os.Getenv("kafka_security_protocol")
	vKafka.sasl_mechanisms = os.Getenv("kafka_sasl_mechanisms")
	vKafka.sasl_username = os.Getenv("kafka_sasl_username")
	vKafka.sasl_password = os.Getenv("kafka_sasl_password")

	vKafka.flush_interval, err = strconv.Atoi(os.Getenv("kafka_flushinterval"))
	if err != nil {
		grpcLog.Error("2 String to Int convert error: %s", err)

	}

	// Lets manage how much we prnt to the screen
	vKafka.numpartitions, err = strconv.Atoi(vKafka_NumPartitions)
	if err != nil {
		grpcLog.Error("vKafka_NumPartitions, String to Int convert error: %s", err)

	}

	// Lets manage how much we prnt to the screen
	vKafka.replicationfactor, err = strconv.Atoi(vKafka_ReplicationFactor)
	if err != nil {
		grpcLog.Error("vKafka_ReplicationFactor, String to Int convert error: %s", err)

	}

	grpcLog.Info("****** Kafka Connection Parameters *****")
	grpcLog.Info("Kafka bootstrap Server is\t", vKafka.bootstrapservers)
	grpcLog.Info("Kafka Topic is\t\t", vKafka.topicname)
	grpcLog.Info("Kafka # Parts is\t\t", vKafka_NumPartitions)
	grpcLog.Info("Kafka Rep Factor is\t\t", vKafka_ReplicationFactor)
	grpcLog.Info("Kafka Retension is\t\t", vKafka.retension)
	grpcLog.Info("Kafka ParseDuration is\t", vKafka.parseduration)

	return vKafka
}

// Pretty Print JSON string
func prettyPrintJSON(v interface{}) {
	tmpBArray, err := json.MarshalIndent(v, "", "    ")
	if err == nil {
		grpcLog.Infof("Message:\n%s\n", tmpBArray)

	} else {
		grpcLog.Error("Really!?!? How is this possible:", err)
		return

	}
}

func prettyJSON(ms string) {

	var obj map[string]interface{}
	//json.Unmarshal([]byte(string(m.Value)), &obj)
	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

func main() {

	//	var person TPperson

	// Initialize the vGeneral struct variable.
	vGeneral := loadGeneralProps()
	vKafka := loadKafkaProps()

	// Create admin client to create the topic if it does not exist
	grpcLog.Info("")
	grpcLog.Info("**** Confirm Topic Existence & Configuration ****")

	// Lets make sure the topic/s exist
	CreateTopic(vKafka)

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	grpcLog.Info("")
	grpcLog.Info("**** Configure Client Kafka Connection ****")

	grpcLog.Info("Kafka bootstrap Server is\t", vKafka.bootstrapservers)

	cm := kafka.ConfigMap{
		"bootstrap.servers":       vKafka.bootstrapservers,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"client.id":               vGeneral.hostname,
	}
	grpcLog.Infoln("Basic Client ConfigMap compiled")

	if vKafka.sasl_mechanisms != "" {
		cm["sasl.mechanisms"] = vKafka.sasl_mechanisms
		cm["security.protocol"] = vKafka.security_protocol
		cm["sasl.username"] = vKafka.sasl_username
		cm["sasl.password"] = vKafka.sasl_password
		grpcLog.Infoln("Security Authentifaction configured in ConfigMap")

	}

	// Variable p holds the new Producer instance.
	p, err := kafka.NewProducer(&cm)

	// Check for errors in creating the Producer
	if err != nil {
		grpcLog.Errorf("😢Oh noes, there's an error creating the Producer! ", err)

		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				grpcLog.Fatalf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err)

			default:
				grpcLog.Fatalf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err)

			}

		} else {
			// It's not a kafka.Error
			grpcLog.Fatalf("😢 Oh noes, there's a generic error creating the Producer! %v", err.Error())

		}
		// call it when you know it's broken
		os.Exit(1)

	} else {
		grpcLog.Infoln("Created Kafka Producer instance :")
		grpcLog.Infoln("")

		///////////////////////////////////////////////////////
		//
		// Successful connection established with Kafka Cluster
		//
		///////////////////////////////////////////////////////

		//
		// For signalling termination from main to go-routine
		termChan := make(chan bool, 1)
		// For signalling that termination is done from go-routine to main
		doneChan := make(chan bool)

		vStart := time.Now()

		vFlush := 0

		// we want to skip the first line as it's a header line
		for count := 0; count < vGeneral.testsize; count++ {

			// Eventually we will have a routine here that will not create fake data, but will rather read the data from
			// a file, do some checks and then publish the JSON payload structured Txn to the topic.

			// if 0 then sleep is disabled otherwise
			//
			// lets get a random value 0 -> vGeneral.sleep, then delay/sleep as up to that fraction of a second.
			// this mimics someone thinking, as if this is being done by a human at a keyboard, for batcvh file processing we don't have this.
			// ie if the user said 200 then it is 1 / 200 of a second or any value between 0 and 200
			// if user said 1000 then it is 1 / 1000 of a second, or any value between 0 and 10000
			// n will be between 0 and 1000  aka 0 and 1 second

			if vGeneral.sleep != 0 {
				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(vGeneral.sleep) // n will be between 0 and 1000  aka 0 and 1 second
				fmt.Printf("Sleeping %d Millisecond...\n", n)
				time.Sleep(time.Duration(n) * time.Millisecond)
			}

			// Start of fake data generate
			// https://github.com/brianvoe/gofakeit
			// https://pkg.go.dev/github.com/brianvoe/gofakeit

			gofakeit.Seed(time.Now().UnixNano())
			gofakeit.Seed(0)

			g_address := gofakeit.Address()
			g_ccard := gofakeit.CreditCard()
			g_job := gofakeit.Job()
			g_contact := gofakeit.Contact()
			g_person := gofakeit.Person()

			address := &TPaddress{
				Streetname: g_address.Street,
				City:       g_address.City,
				State:      g_address.State,
				Zip:        g_address.Zip,
				Country:    g_address.Country,
				Latitude:   g_address.Latitude,
				Longitude:  g_address.Longitude,
			}

			ccard := &TPccard{
				Type:   g_ccard.Type,
				Number: g_ccard.Number,
				Exp:    g_ccard.Exp,
				Cvv:    g_ccard.Cvv,
			}

			job := &TPjobinfo{
				Company:    g_job.Company,
				Title:      g_job.Title,
				Descriptor: g_job.Descriptor,
				Level:      g_job.Level,
			}

			contact := &TPcontact{
				Email: g_contact.Email,
				Phone: g_contact.Phone,
			}

			tperson := &TPperson{
				Ssn:          g_person.SSN,
				Firstname:    g_person.FirstName,
				Lastname:     g_person.LastName,
				Gender:       g_person.Gender,
				Contact:      *contact,
				Address:      *address,
				Ccard:        *ccard,
				Job:          *job,
				Created_date: time.Now().Format("2006-01-02T15:04:05"),
			}

			cBank := getBanks()[gofakeit.Number(1, 15)]
			cTransType := getTransType()[gofakeit.Number(1, 4)]
			cDirection := getDirection()[gofakeit.Number(1, 2)]
			nAmount := fmt.Sprintf("%9.2f", gofakeit.Price(0, 999999999))

			t_PaymentNrt := map[string]interface{}{
				"accountAgentId":  strconv.Itoa(rand.Intn(6)),
				"accountEntityId": strconv.Itoa(rand.Intn(6)),
				"accountId":       strconv.Itoa(gofakeit.CreditCardNumber()),
				"amount": map[string]interface{}{
					"baseCurrency": "zar",
					"baseValue":    nAmount,
					"currency":     "zar",
					"value":        nAmount,
				},
				"counterpartyEntityId":         strconv.Itoa(gofakeit.Number(0, 9999)),
				"counterpartyId":               strconv.Itoa(gofakeit.Number(10000, 19999)),
				"counterpartyName":             *tperson,
				"customerEntityId":             "customerEntityId_1",
				"customerId":                   "customerId_1",
				"direction":                    cDirection,
				"eventId":                      strconv.Itoa(gofakeit.CreditCardNumber()),
				"eventTime":                    time.Now().Format("2006-01-02T15:04:05"),
				"eventType":                    "paymentRT",
				"fromId":                       strconv.Itoa(gofakeit.CreditCardNumber()),
				"instructedAgentId":            strconv.Itoa(gofakeit.Number(0, 4999)),
				"instructingAgentId":           strconv.Itoa(gofakeit.Number(5000, 9999)),
				"msgStatus":                    "msgStatus_1",
				"msgType":                      "msgType_1",
				"numberOfTransactions":         strconv.Itoa(gofakeit.Number(0, 99)),
				"paymentFrequency":             strconv.Itoa(gofakeit.Number(1, 12)),
				"paymentMethod":                "paymentMethod_1",
				"schemaVersion":                "1",
				"serviceLevelCode":             "serviceLevelCode_1",
				"settlementClearingSystemCode": "settlementClearingSystemCode_1",
				"settlementDate":               time.Now().Format("2006-01-02"),
				"settlementMethod":             "samos",
				"tenantId":                     []string{cBank},
				"toId":                         strconv.Itoa(gofakeit.CreditCardNumber()),
				"transactionId":                uuid.New().String(),
				"transactionType":              cTransType,
			}

			er := map[string]interface{}{
				"JsonVersion":      4,
				"OriginatingEvent": t_PaymentNrt,
				"OutputTime":       time.Now().Format("2006-01-02T15:04:05"),
				"ProcessorId":      "proc",
				"Versions": map[string]interface{}{
					"ModelGraph": 4,
					"ConfigGroups": []TPconfigGroups{
						TPconfigGroups{
							Type:    "global",
							Version: "1",
						},
						TPconfigGroups{
							Type:    "analytical",
							Version: "3",
							Id:      "acg1",
						},
						TPconfigGroups{
							Type:    "analytical",
							Version: "2",
							Id:      "acg2",
						},
						TPconfigGroups{
							Type:    "tenant",
							Id:      "tenant10",
							Version: "0",
						},
					},
				},
			}

			// Change/Marshal the person variable int a json object
			valueBytes, err := json.Marshal(er)
			if err != nil {
				grpcLog.Errorln("Marchalling error: ", err)

			}

			if vGeneral.debuglevel > 0 {
				prettyJSON(string(valueBytes))
			}

			kafkaMsg := kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &vKafka.topicname,
					Partition: kafka.PartitionAny,
				},
				Value: valueBytes,    // This is the payload/body thats being posted
				Key:   []byte(cBank), // We us this to group the same transactions together in order, IE submitting/Debtor Bank.
			}

			// We will decide if we want to keep this bit!!! or simplify it.
			//
			// Handle any events that we get
			go func() {
				doTerm := false
				for !doTerm {
					// The `select` blocks until one of the `case` conditions
					// are met - therefore we run it in a Go Routine.
					select {
					case ev := <-p.Events():
						// Look at the type of Event we've received
						switch ev.(type) {

						case *kafka.Message:
							// It's a delivery report
							km := ev.(*kafka.Message)
							if km.TopicPartition.Error != nil {
								if vGeneral.debuglevel > 2 {
									fmt.Printf("☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
										string(km.Value),
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Error)

								}

							} else {
								if vGeneral.debuglevel == 1 {
									//fmt.Println(person.Firstname, " ", person.Lastname, " ", person.Ssn)

								} else if vGeneral.debuglevel == 4 {

									fmt.Printf("✅ Message '%v' delivered to topic '%v'(partition %d at offset %d)\n",
										string(km.Value),
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Partition,
										km.TopicPartition.Offset)
								}
							}

						case kafka.Error:
							// It's an error
							em := ev.(kafka.Error)
							grpcLog.Errorln("☠️ Uh oh, caught an error:\n\t%v\n", em)
							//logCtx.Errorf(fmt.Sprintf("☠️ Uh oh, caught an error:\n\t%v\n", em))

						}
					case <-termChan:
						doTerm = true

					}
				}
				close(doneChan)
			}()

			// This is where we publish message onto the topic... on the cluster
			// Produce the message
			if e := p.Produce(&kafkaMsg, nil); e != nil {
				grpcLog.Errorln("😢 Darn, there's an error producing the message!", e.Error())

			}

			//Fush every flush_interval loops
			if vFlush == vKafka.flush_interval {
				t := 10000
				if r := p.Flush(t); r > 0 {
					grpcLog.Errorf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)

				} else {
					grpcLog.Infoln(vFlush, "Messages flushed from the queue")
					vFlush = 0
				}
			}
			vFlush++

		}

		vEnd := time.Now()

		// --
		// Flush the Producer queue, t = TimeOut, 1000 = 1 second
		t := 10000
		if r := p.Flush(t); r > 0 {
			grpcLog.Errorf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)
			//logCtx.Errorf(fmt.Sprintf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r))

		} else {
			grpcLog.Infoln("\n--\n✨ All messages flushed from the queue")

		}

		fmt.Println("Start      : ", vStart)
		fmt.Println("End        : ", vEnd)
		fmt.Println("Duration   : ", vEnd.Sub(vStart))
		fmt.Println("Records    : ", vGeneral.testsize)

		// --
		// Stop listening to events and close the producer
		// We're ready to finish
		termChan <- true

		// wait for go-routine to terminate
		<-doneChan
		defer p.Close()

	}
}
