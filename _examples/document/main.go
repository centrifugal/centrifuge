package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	fdelta "github.com/shadowspore/fossil-delta"
)

type Event struct {
	Type   string
	Minute int
}

type Player struct {
	Name   string
	Events []Event
}

type Team struct {
	Name    string
	Score   int
	Players [11]Player
}

type Match struct {
	HomeTeam Team
	AwayTeam Team
}

// Define event types
const (
	Goal       = "goal"
	YellowCard = "yellow card"
	RedCard    = "red card"
	Substitute = "substitute"
)

func simulateMatch(match *Match) {
	fmt.Println("Match started between", match.HomeTeam.Name, "and", match.AwayTeam.Name)

	totalSimulationTime := 1                                             // Total time for the simulation in seconds
	totalEvents := 20                                                    // Total number of events to simulate
	eventInterval := float64(totalSimulationTime) / float64(totalEvents) // Time between events

	var prevData []byte
	totalBytes := 0
	totalBytesSent := 0

	for i := 0; i < totalEvents; i++ {
		time.Sleep(time.Duration(eventInterval*1000) * time.Millisecond) // Sleep between events

		minute := int(float64(i) * eventInterval / float64(totalSimulationTime) * 90) // Calculate minute based on event occurrence
		eventType := chooseRandomEventType()
		team := chooseRandomTeam(match)
		playerIndex := rand.Intn(11) // Choose one of the 11 players randomly
		playerName := team.Players[playerIndex].Name

		event := Event{Type: eventType, Minute: minute}
		team.Players[playerIndex].Events = append(team.Players[playerIndex].Events, event)

		if eventType == Goal {
			team.Score++
			fmt.Printf("[%d'] GOAL! %s by %s. New score is %s %d - %d %s\n", minute, team.Name, playerName, match.HomeTeam.Name, match.HomeTeam.Score, match.AwayTeam.Score, match.AwayTeam.Name)
		} else {
			fmt.Printf("[%d'] %s for %s\n", minute, eventType, playerName)
		}

		data, _ := json.Marshal(match)
		totalBytes += len(data)
		if prevData != nil {

			patch, err := jsonpatch.CreateMergePatch(prevData, data)
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Println(string(patch))

			patch = fdelta.Create(prevData, data)
			//fmt.Println(string(patch))
			patch = []byte(base64.StdEncoding.EncodeToString(patch))

			totalBytesSent += len(patch)
		} else {
			totalBytesSent += len(data)
		}
		prevData = data
	}

	fmt.Println(
		"Match ended. Final Score:",
		match.HomeTeam.Name, match.HomeTeam.Score, "-", match.AwayTeam.Score, match.AwayTeam.Name)
	fmt.Println("Bytes without delta:", totalBytes)
	fmt.Println("Bytes with delta:", totalBytesSent)
}

func chooseRandomEventType() string {
	events := []string{Goal, YellowCard, RedCard, Substitute}
	return events[rand.Intn(len(events))]
}

func chooseRandomTeam(match *Match) *Team {
	if rand.Intn(2) == 0 {
		return &match.HomeTeam
	}
	return &match.AwayTeam
}

// Helper function to create players with names from a given list
func assignNamesToPlayers(names []string) [11]Player {
	var players [11]Player
	for i, name := range names {
		players[i] = Player{Name: name}
	}
	return players
}

func main() {
	// Predefined lists of player names for each team
	playerNamesTeamA := []string{"John Doe", "Jane Smith", "Alex Johnson", "Chris Lee", "Pat Kim", "Sam Morgan", "Jamie Brown", "Casey Davis", "Morgan Garcia", "Taylor White", "Jordan Martinez"}
	playerNamesTeamB := []string{"Robin Wilson", "Drew Taylor", "Jessie Bailey", "Casey Flores", "Jordan Walker", "Charlie Green", "Alex Adams", "Morgan Thompson", "Taylor Clark", "Jordan Hernandez", "Jamie Lewis"}

	// Example setup
	match := Match{
		HomeTeam: Team{
			Name:    "Real Madrid",
			Players: assignNamesToPlayers(playerNamesTeamA),
		},
		AwayTeam: Team{
			Name:    "Barcelona",
			Players: assignNamesToPlayers(playerNamesTeamB),
		},
	}

	simulateMatch(&match)
}
