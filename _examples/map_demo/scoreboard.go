package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/centrifugal/centrifuge"
)

type MatchEvent struct {
	Minute int    `json:"minute"`
	Type   string `json:"type"`
	Team   string `json:"team"`
	Player string `json:"player"`
}

type MatchData struct {
	MatchID     string       `json:"match_id"`
	HomeTeam    string       `json:"home_team"`
	AwayTeam    string       `json:"away_team"`
	HomeScore   int          `json:"home_score"`
	AwayScore   int          `json:"away_score"`
	Minute      int          `json:"minute"`
	Status      string       `json:"status"`
	HomePoss    int          `json:"home_poss"`
	HomeShots   int          `json:"home_shots"`
	AwayShots   int          `json:"away_shots"`
	HomePasses  int          `json:"home_passes"`
	AwayPasses  int          `json:"away_passes"`
	HomeCorners int          `json:"home_corners"`
	AwayCorners int          `json:"away_corners"`
	HomeFouls   int          `json:"home_fouls"`
	AwayFouls   int          `json:"away_fouls"`
	HomeYellow  int          `json:"home_yellow"`
	AwayYellow  int          `json:"away_yellow"`
	HomeRed     int          `json:"home_red"`
	AwayRed     int          `json:"away_red"`
	Events      []MatchEvent `json:"events"`
	UpdatedAt   int64        `json:"updated_at"`
}

type matchSim struct {
	id       string
	homeTeam string
	awayTeam string
	data     MatchData
}

var matchConfigs = []struct {
	id   string
	home string
	away string
}{
	{"match1", "Barcelona", "Real Madrid"},
	{"match2", "Liverpool", "Man City"},
	{"match3", "Bayern Munich", "Dortmund"},
	{"match4", "PSG", "Marseille"},
	{"match5", "Juventus", "AC Milan"},
	{"match6", "Ajax", "Feyenoord"},
}

var homePlayers = map[string][]string{
	"Barcelona":     {"Yamal", "Pedri", "Lewandowski", "Gavi", "de Jong"},
	"Liverpool":     {"Salah", "Nunez", "Diaz", "Szoboszlai", "Mac Allister"},
	"Bayern Munich": {"Musiala", "Kane", "Sane", "Kimmich", "Muller"},
	"PSG":           {"Dembele", "Barcola", "Asensio", "Vitinha", "Zaïre-Emery"},
	"Juventus":      {"Vlahovic", "Yildiz", "Chiesa", "Locatelli", "Rabiot"},
	"Ajax":          {"Bergwijn", "Brobbey", "Taylor", "Berghuis", "Blind"},
}

var awayPlayers = map[string][]string{
	"Real Madrid": {"Vinicius", "Bellingham", "Mbappe", "Rodrygo", "Valverde"},
	"Man City":    {"Haaland", "De Bruyne", "Foden", "Grealish", "Silva"},
	"Dortmund":    {"Adeyemi", "Brandt", "Reus", "Sabitzer", "Malen"},
	"Marseille":   {"Aubameyang", "Sanchez", "Harit", "Guendouzi", "Ounahi"},
	"AC Milan":    {"Leao", "Pulisic", "Giroud", "Reijnders", "Theo"},
	"Feyenoord":   {"Gimenez", "Stengs", "Timber", "Kokcu", "Dilrosun"},
}

func newMatch(id, home, away string) *matchSim {
	m := &matchSim{
		id:       id,
		homeTeam: home,
		awayTeam: away,
	}
	m.reset()
	return m
}

func (m *matchSim) reset() {
	m.data = MatchData{
		MatchID:   m.id,
		HomeTeam:  m.homeTeam,
		AwayTeam:  m.awayTeam,
		HomePoss:  50,
		Status:    "1H",
		Events:    []MatchEvent{},
		UpdatedAt: time.Now().UnixMilli(),
	}
}

func (m *matchSim) randomPlayer(team string) string {
	var players []string
	if team == "home" {
		players = homePlayers[m.homeTeam]
	} else {
		players = awayPlayers[m.awayTeam]
	}
	if len(players) == 0 {
		return "Unknown"
	}
	return players[rand.Intn(len(players))]
}

func (m *matchSim) addEvent(evt MatchEvent) {
	m.data.Events = append(m.data.Events, evt)
	// Keep only last 8 events.
	if len(m.data.Events) > 8 {
		m.data.Events = m.data.Events[len(m.data.Events)-8:]
	}
}

// tick advances the match by ~2 minutes of match time.
func (m *matchSim) tick() {
	d := &m.data

	switch d.Status {
	case "HT", "FT":
		return
	}

	d.Minute += 2
	d.UpdatedAt = time.Now().UnixMilli()

	// Transition to half-time or full-time.
	if d.Minute >= 45 && d.Status == "1H" {
		d.Status = "HT"
		d.Minute = 45
		return
	}
	if d.Minute >= 90 && d.Status == "2H" {
		d.Status = "FT"
		d.Minute = 90
		return
	}

	// Pick which team gets action this tick.
	team := "home"
	if rand.Intn(100) >= d.HomePoss {
		team = "away"
	}

	// Possession drift: small random walk.
	d.HomePoss += rand.Intn(5) - 2
	if d.HomePoss < 30 {
		d.HomePoss = 30
	}
	if d.HomePoss > 70 {
		d.HomePoss = 70
	}

	// Passes (always increment both sides).
	d.HomePasses += 8 + rand.Intn(12)
	d.AwayPasses += 8 + rand.Intn(12)

	// Shots (~20% chance per tick).
	if rand.Intn(100) < 20 {
		if team == "home" {
			d.HomeShots++
		} else {
			d.AwayShots++
		}
	}

	// Goal (~3% chance per tick — roughly 2-3 goals per match).
	if rand.Intn(100) < 3 {
		player := m.randomPlayer(team)
		if team == "home" {
			d.HomeScore++
		} else {
			d.AwayScore++
		}
		m.addEvent(MatchEvent{Minute: d.Minute, Type: "goal", Team: team, Player: player})
	}

	// Corner (~8% chance).
	if rand.Intn(100) < 8 {
		if team == "home" {
			d.HomeCorners++
		} else {
			d.AwayCorners++
		}
		m.addEvent(MatchEvent{Minute: d.Minute, Type: "corner", Team: team, Player: m.randomPlayer(team)})
	}

	// Foul (~10% chance, fouling team is the opponent).
	if rand.Intn(100) < 10 {
		fouler := "away"
		if team == "away" {
			fouler = "home"
		}
		if fouler == "home" {
			d.HomeFouls++
		} else {
			d.AwayFouls++
		}

		// Yellow card on ~40% of fouls.
		if rand.Intn(100) < 40 {
			player := m.randomPlayer(fouler)
			if fouler == "home" {
				d.HomeYellow++
			} else {
				d.AwayYellow++
			}
			m.addEvent(MatchEvent{Minute: d.Minute, Type: "yellow", Team: fouler, Player: player})
		}
	}

	// Red card (~0.5% chance — rare.
	if rand.Intn(1000) < 5 {
		player := m.randomPlayer(team)
		if team == "home" {
			d.HomeRed++
		} else {
			d.AwayRed++
		}
		m.addEvent(MatchEvent{Minute: d.Minute, Type: "red", Team: team, Player: player})
	}
}

func publishScoreboardData(ctx context.Context, node *centrifuge.Node) {
	matches := make([]*matchSim, len(matchConfigs))
	for i, cfg := range matchConfigs {
		matches[i] = newMatch(cfg.id, cfg.home, cfg.away)
	}

	// Stagger initial minutes so matches aren't all in sync.
	matches[0].data.Minute = 0
	matches[1].data.Minute = 10
	matches[2].data.Minute = 20
	matches[3].data.Minute = 30
	matches[4].data.Minute = 40
	matches[5].data.Minute = 5

	tc := time.NewTicker(1 * time.Second)
	defer tc.Stop()

	// Track pause timers for HT/FT per match.
	pauseUntil := make([]time.Time, len(matches))

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tc.C:
			for i, m := range matches {
				// Handle paused states.
				if !pauseUntil[i].IsZero() && now.Before(pauseUntil[i]) {
					continue
				}
				pauseUntil[i] = time.Time{}

				switch m.data.Status {
				case "HT":
					// Resume into second half after 5s pause.
					m.data.Status = "2H"
					pauseUntil[i] = now.Add(5 * time.Second)
					continue
				case "FT":
					// Restart match after 10s pause.
					m.reset()
					pauseUntil[i] = now.Add(2 * time.Second)
					continue
				}

				m.tick()

				jsonData, err := json.Marshal(m.data)
				if err != nil {
					log.Printf("Failed to marshal match %s: %v", m.id, err)
					continue
				}
				_, err = node.MapPublish(ctx, "scoreboard", m.id, centrifuge.MapPublishOptions{
					Data:     jsonData,
					UseDelta: true,
				})
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("Failed to publish match %s: %v", m.id, err)
				}
			}
		}
	}
}
