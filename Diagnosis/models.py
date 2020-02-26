from django.db import models


class Diagnosis(models.Model):
    visit_dt = models.DateTimeField(unique=True)
    sex = models.CharField(max_length=1, default='M')
    icd10 = models.CharField(max_length=20)
