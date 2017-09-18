"""Phreak Watch Model

1. Prepare Problem
    a) Load libraries
    b) Load data

2. Summarize Data
    a) Descriptive statistics
    b) Data visualizations

3. Pre-processing Pipeline - For each model we want to train, do the following things and use what helps.

    3.1 Data Cleaning - Garbage in, garbage out.

    3.3 Data Transforms - Make the data digestible to the algorithms.
        Standardize
        Scale
        Binarization
        Normalize
        Imputing

    3.2 Feature Extraction - Generate some features (for each model).
        Polynomial Features
        Vectorizing
        Hashing
        Label Encoding

    3.2 Feature Selection - Select the best features (for each model).
        SelectKBest - GridSearch over k
        SelectFromModel - GridSearch over threshold
        RFE - Recursive Feature Elimination
        PCA - GridSearch over components

    3.4 Feature Union
    feature_union= make_union(SelectKBest(k=6),
                              PCA(n_components=3))

    pipe_model = make_pipeline(feature_union,
                               LinearDiscriminantAnalysis()
                               StandardScaler())

4. Evaluate Algorithms
    kfold = KFold(n_splits=10, random_state=7)
    results = cross_val_score(model, X, Y, cv=kfold)

    a) Split-out validation dataset
        train_test_split()

    b) Test options and evaluation metric
        # evaluate each model in turn
        results = []
        names = []
        scoring = accuracy
        for name, model in models:
          kfold = KFold(n_splits=10, random_state=7)
          cv_results = cross_val_score(model, X, Y, cv=kfold, scoring=scoring)
          results.append(cv_results)
          names.append(name)
          msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
          print(msg)

    c) Spot Check Algorithms

    d) Compare Algorithms
        # boxplot algorithm comparison
        fig = pyplot.figure()
        fig.suptitle( Algorithm Comparison )
        ax = fig.add_subplot(111)
        pyplot.boxplot(results)
        ax.set_xticklabels(names)
        pyplot.show()

5. Improve Accuracy
    a) Algorithm Tuning
    b) Ensembles

6. Finalize Model
    a) Predictions on validation dataset
    b) Create standalone model on entire training dataset
    c) Save model for later use

"""

