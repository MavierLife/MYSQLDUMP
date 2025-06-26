from ui import App

# CONFIGURACIÓN DE TELEGRAM PÚBLICO
telegram_config = {
    'bot_token': '8175238557:AAGXVzpknh0YrEJD288pBvX4CY4Yw-uNwH4',  # Token de tu bot
    'auto_subscribe': True,  # Suscribir automáticamente a nuevos usuarios
    'send_test': True       # Enviar mensaje de prueba a suscriptores existentes
}

if __name__ == "__main__":
    app = App(telegram_config=telegram_config)
    app.mainloop()