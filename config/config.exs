import Config

config :hyparview,
  telemetry_prefix: [:hyparview]

import_config "#{config_env()}.exs"
