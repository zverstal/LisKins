module.exports = {
  apps: [
    {
      name: "skins-arb-bot",
      script: "src/index.js",
      // если используешь ES-модули (у тебя "type":"module"), pm2 это ок
      // интерпретатор Node берёт из системы
      instances: 1,            // можешь поставить "max" для всех ядер; у нас одиночный воркер
      exec_mode: "fork",       // кластеризация нам не нужна (одна WS-сессия к LIS)
      watch: false,            // включай true на dev, но осторожно с перезапусками
      autorestart: true,
      max_memory_restart: "300M",
      // окружение
      env: {
        NODE_ENV: "development"
      },
      env_production: {
        NODE_ENV: "production"
      },
      // логи
      error_file: "logs/pm2-error.log",
      out_file: "logs/pm2-out.log",
      merge_logs: true,
      time: true,
      // graceful shutdown
      kill_timeout: 5000, // даём времени на disconnect сокетов/бота
    }
  ]
};
