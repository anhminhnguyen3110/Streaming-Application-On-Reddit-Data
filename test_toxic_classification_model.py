import requests

API_URL = "https://api-inference.huggingface.co/models/minh21/XLNet-Reddit-Toxic-Comment-Classification"
headers = {"Authorization": "Bearer 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"}


def query(payload):
    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        return response.json()
    except Exception as e:
        return None


def classify_comment(comment):
    output = query({"inputs": comment})

    if output == None:
        return "Error"

    try:
        # Find the label with the highest score
        max_score_label = max(output[0], key=lambda x: x["score"])

        if max_score_label["label"] == "LABEL_1":
            return "Toxic comment"
        else:
            return "Non-toxic comment"
    except Exception as e:
        return "Error"


comment = "Love you"
classification = classify_comment(comment)
print(classification)
