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
	esadProfile             = "ESAD_PROFILE"
)

var profilesCmd = &cobra.Command{
	Use:   profileBaseCmdName + " [flags] [command] [sub command]",
	Short: "profile is a collection of settings and credentials that you can apply to an esad command",
	Long: `	   A named profile is a collection of settings and credentials that you can apply to an ESAD command. 
           When you specify a profile to run a command, the settings and credentials are used to run that command. 
           You can specify a profile in an environment variable (ESAD_PROFILE) which essentially acts as the default profile for commands if default doesn't exists.
           The ESAD CLI supports using any of multiple named profiles that are stored in the config and credentials files.`,
}

var createProfilesCmd = &cobra.Command{
	Use:   createNewProfileCmdName,
	Short: "Create new named profile",
	Long:  `A named profile is a collection of settings and credentials that you can apply to an ESAD command.`,
	Run: func(cmd *cobra.Command, args []string) {
		createProfile()
	},
}

var deleteProfileCmd = &cobra.Command{
	Use:   deleteNewProfileCmdName + " [list of profile names to be deleted]",
	Short: "Delete named profiles",
	Long:  `Delete profile permanently from configuration files`,
	Run: func(cmd *cobra.Command, args []string) {
		//If no args, display usage
		if len(args) < 1 {
			if err := cmd.Usage(); err != nil {
				fmt.Println(err)
			}
			return
		}
		deleteProfiles(args)
	},
}

var listProfilesCmd = &cobra.Command{
	Use:   listProfileCmdName,
	Short: "lists named profiles",
	Long:  `A named profile is a collection of settings and credentials that you can apply to an ESAD command.`,
	Run: func(cmd *cobra.Command, args []string) {
		displayProfiles()
	},
}

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
	fmt.Fprintf(w, "%s\t\t%s\t\t%s\t\n", "----", "--------", "------------")
	for _, profile := range config.Profiles {
		fmt.Fprintf(w, "%s\t\t%s\t\t%s\t\n", profile.Name, profile.Username, profile.Endpoint)
	}
	w.Flush()

}

func init() {
	profilesCmd.AddCommand(createProfilesCmd)
	profilesCmd.AddCommand(deleteProfileCmd)
	esadCmd.AddCommand(profilesCmd)
	profilesCmd.AddCommand(listProfilesCmd)

}

func createProfile() {
	var name string
	for {
		fmt.Printf("Enter profile's name: ")
		name = getUserInput("profile's name", false)
		if !isProfileExists(name) {
			break
		}
		fmt.Println("profile", name, "already exists.")
	}
	fmt.Printf("ES Anomaly Detection Endpoint: ")
	endpoint := getUserInput("endpoint", false)
	fmt.Printf("ES Anomaly Detection User: ")
	user := getUserInput("user", false)
	fmt.Printf("ES Anomaly Detection Password: ")
	password := getUserInput("password", true)
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

func isProfileExists(name string) bool {
	profiles := getProfiles()
	for _, profile := range profiles {
		if profile.Name == name {
			return true
		}
	}
	return false
}

func getUserInputAsText() string {
	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.TrimSpace(response)
}

func getUserInputAsMaskedText() string {
	maskedValue, err := terminal.ReadPassword(0)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println()
	return fmt.Sprintf("%s", maskedValue)
}

func getUserInput(name string, mask bool) string {

	var response string
	if mask {
		response = getUserInputAsMaskedText()
	} else {
		response = getUserInputAsText()
	}
	if len(response) < 1 {
		fmt.Printf("value cannot be empty. Please enter non-empty value for %s: ", name)
		return getUserInput(name, mask)
	}
	return response
}

func deleteProfiles(names []string) {

	var existingProfileNames []string
	for _, name := range names {
		if !isProfileExists(name) {
			fmt.Println("profile", name, "doesn't exists.")
			continue
		}
		existingProfileNames = append(existingProfileNames, name)
	}
	if len(existingProfileNames) < 1 {
		return
	}

	var remainingProfiles []entity.Profile
	profiles := getProfiles()
	for _, p := range profiles {
		toBeDeleted := false
		for _, name := range existingProfileNames {
			if p.Name == name {
				toBeDeleted = true
				break
			}
		}
		if !toBeDeleted {
			remainingProfiles = append(remainingProfiles, p)
		}
	}
	saveProfiles(remainingProfiles)
}

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

//isEmpty checks whether input is empty or not
func isEmpty(value *string) bool {
	if value == nil {
		return true
	}
	if len(*value) < 1 {
		return true
	}
	return false
}

//getUserProfile select's profile from the list of saved profile
/**
1. First priority is passed as parameters
2. Second priority is by flag --profile [name]
3. Third is get default profile from env or profile named "default"
*/
func getUserProfile() (*client.UserConfig, error) {

	endpoint, err := getValue(FlagEndpoint)
	if err != nil {
		return nil, err
	}
	user, err := getValue(FlagUser)
	if err != nil {
		return nil, err
	}
	password, err := getValue(FlagPassword)
	if err != nil {
		return nil, err
	}
	profile, err := getProfileFromFlag()
	if err != nil {
		return nil, err
	}
	if profile == nil {
		profile, err = getDefaultProfile()
		if err != nil {
			return nil, err
		}
	}
	if profile == nil {
		return nil, errors.New("connection details are not set. Set either by passing or set default profile")
	}
	if !isEmpty(endpoint) {
		profile.Endpoint = *endpoint
	}
	if !isEmpty(user) {
		profile.Username = *user
	}
	if !isEmpty(password) {
		profile.Password = *password
	}
	return profile, nil
}

func getProfileFromFlag() (*client.UserConfig, error) {
	profileName, err := esadCmd.Flags().GetString(FlagProfile)
	if err != nil {
		return nil, err
	}
	if len(profileName) < 1 {
		return nil, nil
	}
	profile, err := getProfileByName(profileName)
	if err != nil {
		return nil, err
	}
	return profile, nil

}
func getDefaultProfile() (*client.UserConfig, error) {

	if profileName, ok := os.LookupEnv(esadProfile); ok {
		return getProfileByName(profileName)
	}
	return getUserConfig("default")
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
