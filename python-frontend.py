from flask import Flask, render_template_string

app = Flask(__name__)

# HTML Template (inline)
html = """
<!DOCTYPE html>
<html>
<head>
    <title>Simple Python Frontend</title>
</head>
<body style="text-align:center; padding-top:50px;">
    <h1>Kubernestes pod:1!</h1>
    <button onclick="alert('Button clicked!')">Click Me</button>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(html)

if __name__ == '__main__':
    app.run(debug=True)
