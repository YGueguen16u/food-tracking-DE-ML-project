import torch
from transformers import BertTokenizer, BertForTokenClassification
from torch.optim import Adam
from torch.utils.data import DataLoader, TensorDataset
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import numpy as np
import pandas as pd

def process_data(data):
    data = data.fillna(method="ffill")
    sentences = []
    labels = []

    sentence_tokens = []
    label_tokens = []

    for _, row in data.iterrows():
        if row['Sentence'] == '':
            sentences.append(sentence_tokens)
            labels.append(label_tokens)
            sentence_tokens = []
            label_tokens = []
        else:
            sentence_tokens.append(row['Sentence'])
            label_tokens.append(row['Label'])

    if sentence_tokens and label_tokens:
        sentences.append(sentence_tokens)
        labels.append(label_tokens)

    return sentences, labels

train_data = pd.read_excel('data/train/t1.xlsx')
val_data = pd.read_excel('data/validation/v1.xlsx')
test_data = pd.read_excel('data/test/te1.xlsx')

train_sentences, train_labels = process_data(train_data)
val_sentences, val_labels = process_data(val_data)
test_sentences, test_labels = process_data(test_data)

# Créez un ensemble unique de labels en utilisant toutes les données
all_labels = train_labels + val_labels + test_labels
unique_labels = set(label for labels in all_labels for label in labels)

label2idx = {label: idx for idx, label in enumerate(unique_labels)}
idx2label = {idx: label for label, idx in label2idx.items()}


PRETRAINED_MODEL_NAME = "bert-base-uncased"
MAX_SEQ_LEN = 128
BATCH_SIZE = 32
EPOCHS = 6

tokenizer = BertTokenizer.from_pretrained(PRETRAINED_MODEL_NAME)
model = BertForTokenClassification.from_pretrained(PRETRAINED_MODEL_NAME, num_labels=len(unique_labels))

def tokenize_and_encode_data(sentences, labels):
    input_ids, attention_masks, label_ids = [], [], []

    for sent, label in zip(sentences, labels):
        encoded_sent = tokenizer.encode_plus(
            sent,
            add_special_tokens=True,
            max_length=MAX_SEQ_LEN,
            padding="max_length",
            return_attention_mask=True,
            return_tensors="pt",
            truncation=True,
        )
        input_ids.append(encoded_sent["input_ids"])
        attention_masks.append(encoded_sent["attention_mask"])
        label_ids.append([label2idx[l] for l in label] + [0] * (MAX_SEQ_LEN - len(label)))

    input_ids = torch.cat(input_ids, dim=0)
    attention_masks = torch.cat(attention_masks, dim=0)
    label_ids = torch.tensor(label_ids)

    return input_ids, attention_masks, label_ids

train_inputs, train_masks, train_labels = tokenize_and_encode_data(train_sentences, train_labels)
val_inputs, val_masks, val_labels = tokenize_and_encode_data(val_sentences, val_labels)

train_data = TensorDataset(train_inputs, train_masks, train_labels)
train_dataloader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)

val_data = TensorDataset(val_inputs, val_masks, val_labels)
val_dataloader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

optimizer = Adam(model.parameters(), lr=3e-5)

def evaluate_model(dataloader):
    model.eval()
    predictions, true_labels = [], []

    for batch in dataloader:
        input_ids, attention_masks, labels = tuple(t.to(device) for t in batch)

        with torch.no_grad():
            outputs = model(input_ids, attention_mask=attention_masks)

        logits = outputs[0].detach().cpu().numpy()
        label_ids = labels.to("cpu").numpy()

        batch_preds = np.argmax(logits, axis=2)
        predictions.extend([list(p) for p in batch_preds])
        true_labels.extend([list(l) for l in label_ids])

    # Supprimer les padding tokens
    predictions = [[idx2label[p] for p, l in zip(prediction, label) if label2idx["O"] != l] for prediction, label in zip(predictions, true_labels)]
    true_labels = [[idx2label[l] for l in label if label2idx["O"] != l] for label in true_labels]

    return predictions, true_labels


for epoch in range(EPOCHS):
    # Entraînement
    model.train()
    for batch in train_dataloader:
        input_ids, attention_masks, labels = tuple(t.to(device) for t in batch)
        outputs = model(input_ids, attention_mask=attention_masks, labels=labels)
        loss = outputs[0]
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

    # Validation
    model.eval()
    val_loss = 0
    for batch in val_dataloader:
        input_ids, attention_masks, labels = tuple(t.to(device) for t in batch)
        outputs = model(input_ids, attention_mask=attention_masks, labels=labels)
        val_loss += outputs[0].item()
    avg_val_loss = val_loss / len(val_dataloader)
    print(f"Epoch {epoch + 1}, Validation loss: {avg_val_loss}")

test_inputs, test_masks, test_labels = tokenize_and_encode_data(test_sentences, test_labels)
test_data = TensorDataset(test_inputs, test_masks, test_labels)
test_dataloader = DataLoader(test_data, batch_size=BATCH_SIZE, shuffle=False)

predictions, true_labels = evaluate_model(test_dataloader)

# Calculez les métriques de performance
flat_true_labels = [label for sublist in true_labels for label in sublist]
flat_predictions = [label for sublist in predictions for label in sublist]

accuracy = accuracy_score(flat_true_labels, flat_predictions)
precision = precision_score(flat_true_labels, flat_predictions, average="micro")
recall = recall_score(flat_true_labels, flat_predictions, average="micro")
f1 = f1_score(flat_true_labels, flat_predictions, average="micro")

print(f"Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}, F1 Score: {f1}")

model.save_pretrained("food_tracking_model")
tokenizer.save_pretrained("food_tracking_model")

def predict(text):
    model.eval()
    input_ids = tokenizer.encode(text, return_tensors="pt").to(device)
    attention_mask = (input_ids != 0).float().to(device)
    with torch.no_grad():
        outputs = model(input_ids, attention_mask=attention_mask)
    logits = outputs[0].detach().cpu().numpy()
    predictions = np.argmax(logits, axis=2)[0]
    tokens = tokenizer.convert_ids_to_tokens(input_ids[0])
    tags = [idx2label[p] for p in predictions]

    return list(zip(tokens, tags))

text = "j'ai mange un burger ce midi."
result = predict(text)
print(result)
