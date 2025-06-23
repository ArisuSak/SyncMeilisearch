package postgres

type ApplicationConfig struct {
    Initialize   bool          `yaml:"initialize"`
    Database     DatabaseConfig `yaml:"database"`
}

type DatabaseConfig struct {
    Host     string `yaml:"host"`
    Port     string `yaml:"port"`
    Name     string `yaml:"database"`
    User     string `yaml:"user"`
    Password string `yaml:"password"`
}