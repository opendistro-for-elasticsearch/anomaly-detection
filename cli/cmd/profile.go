/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package cmd

import (
	"errors"
	"esad/internal/client"
	entity "esad/internal/entity/ad"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh/terminal"
	"os"
	"strings"
	"text/tabwriter"
)

const (
	profileBaseCmdName      = "profile"
	createNewProfileCmdName = "create"
	deleteNewProfileCmdName = "delete"
	listProfileCmdName      = "list"
)

// profilesCmd represents the profiles command
var profilesCmd = &cobra.Command{
	Use:   profileBaseCmdName + " [flags] [command] [sub command]",
	Short: "profile is a collection of settings and credentials that you can apply to an esad command",
	Long: `	   A named profile is a collection of settings and credentials that you can apply to an ESAD command. 
           When you specify a profile to run a command, the settings and credentials are used to run that command. 
           You can specify a profile in an environment variable (ESAD_PROFILE) which essentially acts as the default profile for commands if default doesn't exists.
           The ESAD CLI supports using any of multiple named profiles that are stored in the config and credentials files.`,
	//Run: func(cmd *cobra.Command, args []string) {
	//	v, _ := cmd.Flags().GetBool("nameonly")
	//	displayProfiles(!v)
	//},
}

// createProfilesCmd represents add profiles command
var createProfilesCmd = &cobra.Command{
	Use:   createNewProfileCmdName,
	Short: "Create new named profile",
	Long:  `A named profile is a collection of settings and credentials that you can apply to an ESAD command.`,
	Run: func(cmd *cobra.Command, args []string) {
		createProfile()
	},
}

// deleteProfileCmd represents deleting profiles
var deleteProfileCmd = &cobra.Command{
	Use:   deleteNewProfileCmdName + " [list of profile names to be deleted]",
	Short: "Delete named profiles",
	Long:  `Delete profile permanently from configuration files`,
	Run: func(cmd *cobra.Command, args []string) {
		deleteProfiles(args)
	},
}

// listProfilesCmd represents lists profiles
var listProfilesCmd = &cobra.Command{
	Use:   listProfileCmdName,
	Short: "lists named profiles",
	Long:  `A named profile is a collection of settings and credentials that you can apply to an ESAD command.`,
	Run: func(cmd *cobra.Command, args []string) {
		displayProfiles()
	},
}

// profilesCmd represents the profiles command
//var updateProfileCmd = &cobra.Command{
//	Use:   "edit",
//	Short: "edit profile",
//	Long: `A longer description that spans multiple lines and likely contains examples
//and usage of using your command. For example:
//
//Cobra is a CLI library for Go that empowers applications.
//This application is a tool to generate the needed files
//to quickly create a Cobra application.`,
//	Run: func(cmd *cobra.Command, args []string) {
//		updateProfiles(args[0])
//	},
//}

func displayProfiles() {
	config := &entity.Configuration{
		Profiles: []entity.Profile{},
	}
	err := mapstructure.Decode(viper.AllSettings(), config)
	if err != nil {
		fmt.Println("failed to load config due to ", err)
		return
	}
	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "Name\t\tUserName\t\tEndpoint-url\t")
	for _, profile := range config.Profiles {
		fmt.Fprintf(w, "%s\t\t%s\t\t%s\t\n", profile.Name, profile.Username, profile.Endpoint)
	}
	w.Flush()

}

func init() {
	profilesCmd.AddCommand(createProfilesCmd)
	profilesCmd.AddCommand(deleteProfileCmd)
	//profilesCmd.AddCommand(updateProfileCmd)
	esadCmd.AddCommand(profilesCmd)
	profilesCmd.AddCommand(listProfilesCmd)

}

func createProfile() {
	var name string
	for {
		fmt.Printf("Enter profile's name: ")
		name = userInput("profile's name", false, false)
		if !validProfileName(name) {
			break
		}
		fmt.Println("profile", name, "already exists.")
	}
	fmt.Printf("ES Anomaly Detection Endpoint: ")
	endpoint := userInput("endpoint", false, false)
	fmt.Printf("ES Anomaly Detection User: ")
	user := userInput("user", false, false)
	fmt.Printf("ES Anomaly Detection Password: ")
	password := userInput("password", true, false)
	profile := entity.Profile{
		Name:     name,
		Endpoint: endpoint,
		Username: user,
		Password: password,
	}
	config := &entity.Configuration{
		Profiles: []entity.Profile{},
	}
	err := mapstructure.Decode(viper.AllSettings(), config)
	if err != nil {
		fmt.Println("failed to load profiles due to ", err)
	}
	config.Profiles = append(config.Profiles, profile)
	saveProfiles(config.Profiles)
}

func saveProfiles(profiles []entity.Profile) {
	viper.Set("profiles", profiles)
	err := viper.WriteConfig()
	if err == nil {
		return
	}
	err = viper.SafeWriteConfig()
	if err != nil {
		fmt.Println("failed to save profile due to ", err)
	}
}

func validProfileName(name string) bool {
	profiles := getProfiles()
	for _, profile := range profiles {
		if profile.Name == name {
			return true
		}
	}
	return false
}

func getText() string {
	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.TrimSpace(response)
}

func getMaskedText() string {
	maskedValue, err := terminal.ReadPassword(0)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println()
	return fmt.Sprintf("%s", maskedValue)
}

func userInput(name string, mask bool, allowBlank bool) string {

	var response string
	if mask {
		response = getMaskedText()
	} else {
		response = getText()
	}
	if !allowBlank && len(response) < 1 {
		fmt.Printf("value cannot be empty. Please enter non-empty value for %s: ", name)
		return userInput(name, mask, allowBlank)
	}
	return response
}

func deleteProfiles(names []string) {

	var validProfiles []string
	for _, name := range names {
		if !validProfileName(name) {
			fmt.Println("profile", name, "doesn't exists.")
			continue
		}
		validProfiles = append(validProfiles, name)
	}

	var safeProfiles []entity.Profile
	profiles := getProfiles()
	for _, p := range profiles {
		safe := true
		for _, name := range validProfiles {
			if p.Name == name {
				safe = false
				break
			}
		}
		if safe {
			safeProfiles = append(safeProfiles, p)
		}
	}
	saveProfiles(safeProfiles)
}

//func updateProfiles(name string) {
//
//	profile, err := getProfileByName(name)
//	if err != nil {
//		fmt.Println(err)
//	}
//	fmt.Println("Press Enter if you don't want to update")
//	fmt.Printf("ES Anomaly Detection Endpoint [%s]: ", profile.Endpoint)
//	endpoint := userInput("endpoint", false, true)
//	fmt.Printf("ES Anomaly Detection User [%s]: ", profile.Username)
//	user := userInput("user", false, true)
//	fmt.Printf("ES Anomaly Detection Password : ")
//	password := userInput("password", true, true)
//	if len(endpoint) > 1 {
//		profile.Endpoint = endpoint
//	}
//	if len(user) > 1 {
//		profile.Username = user
//	}
//	if len(password) > 1 {
//		profile.Password = password
//	}
//	config := &entity.Configuration{
//		Profiles: []entity.Profile{},
//	}
//	err = mapstructure.Decode(viper.AllSettings(), config)
//	if err != nil {
//		fmt.Println("failed to load profiles due to ", err)
//	}
//	index := 0
//	for i, p := range config.Profiles {
//		if p.Name == name {
//			index = i
//			break
//		}
//	}
//	config.Profiles[index] = entity.Profile{
//		Endpoint: profile.Endpoint,
//		Username: profile.Username,
//		Password: profile.Password,
//		Name:     name,
//	}
//	saveProfiles(config.Profiles)
//
//}

func getProfiles() []entity.Profile {
	config := &entity.Configuration{
		Profiles: []entity.Profile{},
	}
	err := mapstructure.Decode(viper.AllSettings(), config)
	if err != nil {
		fmt.Println("failed to load config due to ", err)
		return []entity.Profile{}
	}
	return config.Profiles
}

func getValue(flagName string) (*string, error) {
	val, err := esadCmd.Flags().GetString(flagName)
	if err != nil {
		return nil, err
	}
	if len(val) > 0 {
		return &val, nil
	}
	return nil, err
}

func getUserProfile() (*client.UserConfig, error) {

	endpoint, err := getValue("endpoint")
	if err != nil {
		return nil, err
	}
	user, err := getValue("user")
	if err != nil {
		return nil, err
	}
	password, err := getValue("password")
	if err != nil {
		return nil, err
	}
	res, err := getProfileFromFlag()
	if err != nil {
		return nil, err
	}
	if res == nil {
		res, err = getProfileByEnv()
		if err != nil {
			return nil, err
		}
	}
	if res == nil {
		return nil, errors.New("connection details are not set. Set either by passing or set default profile")
	}
	if endpoint != nil && len(*endpoint) > 0 {
		res.Endpoint = *endpoint
	}
	if user != nil && len(*user) > 0 {
		res.Username = *user
	}
	if password != nil && len(*password) > 0 {
		res.Password = *password
	}
	return res, nil
}

func getProfileFromFlag() (*client.UserConfig, error) {
	profileName, err := esadCmd.Flags().GetString("profile")
	if err != nil {
		return nil, err
	}
	if len(profileName) < 1 {
		return nil, nil
	}
	p, err := getProfileByName(profileName)
	if err != nil {
		return nil, err
	}
	return p, nil

}
func getProfileByEnv() (*client.UserConfig, error) {
	profileName := os.Getenv("ESAD_PROFILE")

	if len(profileName) < 1 {
		return getUserConfig("default")
	}
	return getProfileByName(profileName)
}

func getProfileByName(profileName string) (*client.UserConfig, error) {
	userConfig, err := getUserConfig(profileName)
	if err == nil && userConfig == nil {
		return nil, fmt.Errorf("no profile found for name: %s", profileName)
	}
	return userConfig, err
}

func getUserConfig(profileName string) (*client.UserConfig, error) {
	config := &entity.Configuration{
		Profiles: []entity.Profile{},
	}
	err := mapstructure.Decode(viper.AllSettings(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to load config due to %s", err)
	}
	if len(config.Profiles) == 0 {
		return nil, errors.New("no profiles found in config. Add profiles using add command")
	}
	for _, userConfig := range config.Profiles {
		if userConfig.Name == profileName {
			return &client.UserConfig{
				Endpoint: userConfig.Endpoint,
				Username: userConfig.Username,
				Password: userConfig.Password,
			}, nil
		}
	}
	return nil, nil
}
